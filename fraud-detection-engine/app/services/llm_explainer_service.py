"""LLM-based natural-language explainer using Ollama.

Architectural rule: the LLM is NEVER asked to interpret raw numbers or
technical names. All translation of rule flags and SHAP features into plain
sentences happens deterministically first (app.core.explanation_templates,
using real feature values and — for shared phone/account rules — the real
names of the other beneficiaries involved). The LLM's only job is to
rephrase that list of already-simple facts into 1-2 fluent sentences aimed
at a non-technical fraud officer. If the LLM is unreachable or fails, the
deterministic fact list is joined into a plain sentence and returned as-is,
so the officer never sees an error or a blank field.
"""
import json
import logging
import os
import urllib.request
import urllib.error

from app.core.explanation_templates import translate_rule_flags, translate_shap_factor

logger = logging.getLogger(__name__)


class LLMExplainer:
    """Generate human-readable explanations via a local Ollama instance."""

    def __init__(self) -> None:
        self.base_url = os.getenv("OLLAMA_URL", "http://ollama:11434")
        self.model = os.getenv("OLLAMA_MODEL", "llama3.2:1b")
        self.timeout = int(os.getenv("OLLAMA_TIMEOUT", "30"))
        self.language = os.getenv("LLM_LANGUAGE", "fr")  # 'fr' or 'en'

    def explain_case(self, case: dict, shared_entities: dict | None = None) -> str:
        """Return a natural-language explanation for the case dict.

        Always returns a non-empty string: if the LLM is unavailable or
        fails, the deterministic fallback sentence is returned instead of
        an empty string or an error.
        """
        name = self._beneficiary_name(case)
        facts = self._build_facts(case, shared_entities or {})

        if not facts:
            return f"{name} shows no particular risk signal."

        fallback = f"{name}: " + "; ".join(facts) + "."

        prompt = self._build_prompt(name, case, facts)
        try:
            result = self._call_ollama(prompt)
            return result if result else fallback
        except urllib.error.URLError as e:
            logger.warning("Ollama unreachable: %s", e)
            return fallback
        except Exception as e:
            logger.warning("LLM explanation failed: %s", e)
            return fallback

    # ── deterministic fact-building (no LLM involved) ───────────────────────

    @staticmethod
    def _beneficiary_name(case: dict) -> str:
        name = case.get("beneficiary_name") or case.get("name")
        if name:
            return LLMExplainer._humanize_name(name)
        bid = case.get("beneficiary_id", "?")
        return f"Beneficiary #{bid}"

    @staticmethod
    def _humanize_name(raw_name: str) -> str:
        """OpenG2P stores names as "LASTNAME, Firstname" — turn that into a
        natural "Firstname Lastname" so it reads like a sentence subject and
        the LLM doesn't mistake the comma for two different people.
        """
        name = (raw_name or "").strip()
        if "," in name:
            last, _, first = name.partition(",")
            first, last = first.strip(), last.strip()
            if first and last:
                return f"{first} {last.title()}"
        return name

    def _build_facts(self, case: dict, shared_entities: dict) -> list[str]:
        """Deterministically turn rules/features into plain-language facts.

        This is the ONLY place technical data is translated — never inside
        the LLM prompt. Order: named shared-entity facts first (most
        concrete/legible), then rule-based facts, then top SHAP factors.
        """
        rules = case.get("rules_triggered") or []
        if isinstance(rules, str):
            try:
                rules = json.loads(rules)
            except (ValueError, TypeError):
                rules = []

        features = case.get("features") or {}
        if isinstance(features, str):
            try:
                features = json.loads(features)
            except (ValueError, TypeError):
                features = {}

        top_features = case.get("top_features") or []
        if isinstance(top_features, str):
            try:
                top_features = json.loads(top_features)
            except (ValueError, TypeError):
                top_features = []

        network_facts = []
        bank_with = [self._humanize_name(n) for n in (shared_entities.get("shared_bank_with") or [])]
        phone_with = [self._humanize_name(n) for n in (shared_entities.get("shared_phone_with") or [])]
        if bank_with:
            others = ", ".join(bank_with[:3])
            network_facts.append(f"shares a bank account with {others}")
        if phone_with:
            others = ", ".join(phone_with[:3])
            network_facts.append(f"shares a phone number with {others}")

        # "network"/"shared" features are already covered by the named
        # network_facts above (or by rule_sentences quoting a count) — the
        # generic heuristic fallback in _relevant_features would otherwise
        # add a redundant, less useful "shares a bank account with other
        # people" right after the version with real names.
        covered_topics = set()
        if bank_with:
            covered_topics.add("shared_account_count")
        if phone_with:
            covered_topics.add("shared_phone_count")

        rule_sentences = translate_rule_flags(
            rules if isinstance(rules, list) else [], context=features
        )

        factor_sentences = []
        for f in self._relevant_features(top_features)[:3]:
            if not isinstance(f, dict):
                continue
            feature_name = f.get("feature", "")
            if feature_name in covered_topics:
                continue
            sentence = translate_shap_factor(
                feature_name, f.get("value"), f.get("direction", "")
            )
            if sentence:
                factor_sentences.append(sentence)

        # Preserve order, drop duplicates (e.g. a rule and a SHAP factor
        # both describing the same underlying signal).
        return list(dict.fromkeys(network_facts + rule_sentences + factor_sentences))

    _RISK_LABELS_FR = {
        "CRITICAL": "très élevé",
        "HIGH": "élevé",
        "MEDIUM": "modéré",
        "LOW": "faible",
    }
    _RISK_LABELS_EN = {
        "CRITICAL": "very high",
        "HIGH": "high",
        "MEDIUM": "moderate",
        "LOW": "low",
    }

    def _build_prompt(self, name: str, case: dict, facts: list[str]) -> str:
        risk_code = str(case.get("risk_level", "")).upper()
        facts_block = "\n".join(f"- {fact}" for fact in facts)

        if self.language == "en":
            risk = self._RISK_LABELS_EN.get(risk_code, "elevated")
            return (
                "You are helping a non-technical social worker understand a "
                "fraud alert.\n\n"
                f"Beneficiary: {name}\n"
                f"Risk: {risk}\n\n"
                "Observed facts (already in plain language):\n"
                f"{facts_block}\n\n"
                "STRICT RULES:\n"
                "- Do NOT use any technical number (no score, no raw percentage, "
                "no SHAP value)\n"
                "- Do NOT use any jargon (no \"feature\", \"SHAP\", \"threshold\", "
                "\"impact\", no risk-level codes like \"HIGH\" or \"CRITICAL\")\n"
                "- Do NOT use bullet points or a list — write flowing prose only\n"
                "- ALWAYS use the beneficiary's name and the names of other "
                "people mentioned above\n"
                "- Maximum 2 sentences total\n"
                "- End with a simple recommendation: \"needs review\" or "
                "\"priority review recommended\"\n\n"
                "Example of the expected style:\n"
                "\"Halim shares his bank account with Karim, another beneficiary "
                "in the same program. He is also enrolled in an unusual number "
                "of programs at the same time. Priority review recommended.\"\n\n"
                "Write the explanation now, in English, following that exact style:"
            )
        risk = self._RISK_LABELS_FR.get(risk_code, "élevé")
        return (
            "Vous aidez un agent social non technique à comprendre une alerte "
            "de fraude.\n\n"
            f"Bénéficiaire : {name}\n"
            f"Risque : {risk}\n\n"
            "Faits observés (déjà en langage simple) :\n"
            f"{facts_block}\n\n"
            "RÈGLES STRICTES :\n"
            "- N'utilisez PAS de puces ni de liste — rédigez seulement du texte "
            "suivi\n"
            "- N'utilisez PAS de code de niveau de risque brut (\"HIGH\", "
            "\"CRITICAL\", etc.)\n"
            "- N'utilisez AUCUN chiffre technique (pas de score, pas de "
            "pourcentage brut, pas de valeur SHAP)\n"
            "- N'utilisez AUCUN jargon (pas de \"feature\", \"SHAP\", "
            "\"seuil\", \"impact\")\n"
            "- Utilisez TOUJOURS le prénom du bénéficiaire et les noms des "
            "autres personnes mentionnées ci-dessus\n"
            "- Maximum 2 phrases\n"
            "- Terminez par une recommandation simple : \"à vérifier\" ou "
            "\"examen prioritaire recommandé\"\n\n"
            "Exemple du style attendu :\n"
            "\"Halim partage son compte bancaire avec Karim, un autre "
            "bénéficiaire du même programme. Il est également inscrit dans un "
            "nombre inhabituel de programmes en même temps. Examen prioritaire "
            "recommandé.\"\n\n"
            "Rédigez l'explication maintenant, en français, en suivant "
            "exactement ce style :"
        )

    @staticmethod
    def _shap_magnitude(feature: dict) -> float:
        """Read the SHAP contribution under either key name.

        The two explainers in the codebase emit the value under different keys:
        services/explainability_service.py uses 'shap_value', while
        core/shap_explainer.py uses 'impact'. Accept both so relevance
        filtering never silently treats a real contribution as zero.
        """
        val = feature.get("shap_value")
        if val is None:
            val = feature.get("impact", 0)
        try:
            return float(val or 0)
        except (TypeError, ValueError):
            return 0.0

    def _relevant_features(self, top_features) -> list:
        """Drop noise before turning features into facts.

        A feature whose raw value is 0 carries no real signal even if SHAP
        assigns it a contribution (it's an artefact of missing data, e.g.
        income=0) — including it would produce a misleading fact like
        "declares a very low income per person" for someone with no income
        data at all.
        """
        if isinstance(top_features, list):
            items = top_features
        else:
            items = []
        relevant = []
        for f in items:
            if not isinstance(f, dict):
                continue
            try:
                value = float(f.get("value") or 0)
            except (TypeError, ValueError):
                value = 0.0
            if value == 0.0:
                continue
            relevant.append(f)
        return relevant

    def _call_ollama(self, prompt: str) -> str:
        url = f"{self.base_url}/api/generate"
        body = json.dumps({
            "model": self.model,
            "prompt": prompt,
            "stream": False,
            "options": {"temperature": 0.2, "num_predict": 200},
        }).encode("utf-8")

        req = urllib.request.Request(
            url, data=body, headers={"Content-Type": "application/json"}
        )
        with urllib.request.urlopen(req, timeout=self.timeout) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
        return (payload.get("response") or "").strip()
