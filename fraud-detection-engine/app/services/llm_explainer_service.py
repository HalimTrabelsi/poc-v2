"""LLM-based natural-language explainer using Ollama.

Translates a structured fraud decision into a paragraph a fraud officer
can read without ML background. Falls back gracefully if Ollama is
unreachable so the API keeps working.
"""
import json
import logging
import os
import urllib.request
import urllib.error

logger = logging.getLogger(__name__)


class LLMExplainer:
    """Generate human-readable explanations via a local Ollama instance."""

    def __init__(self) -> None:
        self.base_url = os.getenv("OLLAMA_URL", "http://ollama:11434")
        self.model = os.getenv("OLLAMA_MODEL", "llama3.2:1b")
        self.timeout = int(os.getenv("OLLAMA_TIMEOUT", "30"))
        self.language = os.getenv("LLM_LANGUAGE", "fr")  # 'fr' or 'en'

    def explain_case(self, case: dict) -> str:
        """Return a natural-language explanation for the case dict.

        Always returns a string; empty if Ollama is unavailable or fails.
        """
        prompt = self._build_prompt(case)
        try:
            return self._call_ollama(prompt)
        except urllib.error.URLError as e:
            logger.warning("Ollama unreachable: %s", e)
            return ""
        except Exception as e:
            logger.warning("LLM explanation failed: %s", e)
            return ""

    def _build_prompt(self, case: dict) -> str:
        bid = case.get("beneficiary_id", "?")
        score = case.get("final_score", 0)
        risk = case.get("risk_level", "?")
        rec = case.get("recommendation", "?")

        rules = case.get("rules_triggered") or []
        if isinstance(rules, str):
            try:
                rules = json.loads(rules)
            except (ValueError, TypeError):
                rules = []

        rules_text = "\n".join(
            f"- {r.get('name', r.get('rule_id', '?'))}: {r.get('explanation', '')}"
            for r in (rules if isinstance(rules, list) else [])
            if isinstance(r, dict)
        ) or "(none)"

        top_features = case.get("top_features") or []
        if isinstance(top_features, str):
            try:
                top_features = json.loads(top_features)
            except (ValueError, TypeError):
                top_features = []
        feat_text = "\n".join(
            f"- {f.get('feature', '?')}: value={f.get('value', '?')} "
            f"({f.get('direction', '?')}, shap={self._shap_magnitude(f):.3f})"
            for f in self._relevant_features(top_features)
        ) or "(none)"

        if self.language == "en":
            return (
                "You are a fraud-detection assistant explaining a beneficiary risk "
                "assessment to a non-technical fraud officer working on a social "
                "protection programme (OpenG2P).\n\n"
                f"Beneficiary ID: {bid}\n"
                f"Risk score: {float(score):.3f} (scale 0-1)\n"
                f"Risk level: {risk}\n"
                f"Recommendation: {rec}\n\n"
                f"Rules triggered:\n{rules_text}\n\n"
                f"Top contributing features:\n{feat_text}\n\n"
                "Write a 3-4 sentence explanation in plain English. Start with the "
                "verdict, then the top one or two reasons in everyday language, "
                "then a concrete next step the officer should take. Do not include "
                "markdown, headers, or bullet points."
            )
        # Default: French
        return (
            "Vous êtes un assistant de détection de fraude qui explique une "
            "évaluation de risque à un agent non technique travaillant sur un "
            "programme de protection sociale (OpenG2P).\n\n"
            f"Identifiant bénéficiaire : {bid}\n"
            f"Score de risque : {float(score):.3f} (échelle 0-1)\n"
            f"Niveau de risque : {risk}\n"
            f"Recommandation : {rec}\n\n"
            f"Règles déclenchées :\n{rules_text}\n\n"
            f"Facteurs principaux :\n{feat_text}\n\n"
            "Rédigez une explication de 3 à 4 phrases en français simple. "
            "Commencez par le verdict, puis donnez les une ou deux raisons "
            "principales en langage courant, et terminez par l'action concrète "
            "que l'agent doit entreprendre. Pas de markdown, d'en-têtes ni de "
            "puces. Répondez uniquement en français."
        )

    @staticmethod
    def _shap_magnitude(feature: dict) -> float:
        """Read the SHAP contribution under either key name.

        The two explainers in the codebase emit the value under different keys:
        services/explainability_service.py uses 'shap_value', while
        core/shap_explainer.py uses 'impact'. Accept both so the prompt never
        silently shows shap=0.000.
        """
        val = feature.get("shap_value")
        if val is None:
            val = feature.get("impact", 0)
        try:
            return float(val or 0)
        except (TypeError, ValueError):
            return 0.0

    def _relevant_features(self, top_features) -> list:
        """Drop noise before showing features to the LLM.

        A feature whose raw value is 0 carries no real signal even if SHAP
        assigns it a contribution (it's an artefact of missing data, e.g.
        income=0). Keeping such rows makes the LLM invent reasons like
        'income decreases the risk', which confuses fraud officers.
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
            if value == 0.0 and abs(self._shap_magnitude(f)) < 1e-6:
                continue
            if value == 0.0:
                # Non-zero SHAP on a zero feature is the missing-data artefact.
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
