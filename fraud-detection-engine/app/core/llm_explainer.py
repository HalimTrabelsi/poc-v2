"""LLM Explainer — Ollama Mistral explanations in French"""
import httpx
import os


OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://ollama:11434")
LLM_MODEL   = os.getenv("LLM_MODEL",   "mistral")


class LLMExplainer:
    def __init__(self):
        self.base_url = OLLAMA_HOST
        self.model    = LLM_MODEL

    def explain(
        self,
        beneficiary_id: str,
        score: float,
        level: str,
        factors: list[dict],
        flags: list[str],
    ) -> str:
        factors_text = "\n".join([
            f"- {f['feature']}: impact {f['impact']:+.2f} ({f['direction']})"
            for f in factors[:3]
        ])
        flags_text = ", ".join(flags) if flags else "aucun flag"

        prompt = (
            f"Tu es expert en detection de fraudes dans les programmes "
            f"de protection sociale.\n\n"
            f"Beneficiaire ID: {beneficiary_id}\n"
            f"Score de risque: {score:.0%} (niveau: {level})\n\n"
            f"Facteurs detectes par le modele IA:\n{factors_text}\n\n"
            f"Alertes regles metier: {flags_text}\n\n"
            f"En 2 phrases claires en francais, explique pourquoi ce "
            f"beneficiaire est suspect et quelle action recommander "
            f"a l'agent social. Pas de jargon technique."
        )

        try:
            resp = httpx.post(
                f"{self.base_url}/api/generate",
                json={"model": self.model, "prompt": prompt, "stream": False},
                timeout=30.0,
            )
            resp.raise_for_status()
            return resp.json().get("response", "").strip()
        except Exception:
            return self._fallback(level, factors, flags)

    def _fallback(
        self, level: str, factors: list, flags: list
    ) -> str:
        top = factors[0]["feature"] if factors else "inconnu"
        f   = ", ".join(flags) if flags else "aucune alerte"
        return (
            f"Niveau de risque {level}. "
            f"Facteur principal: {top}. "
            f"Alertes declenchees: {f}. "
            f"Une revue manuelle est recommandee."
        )