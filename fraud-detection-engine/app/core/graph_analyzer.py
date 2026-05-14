import logging
import time
from typing import Dict, Any

try:
    import networkx as nx
    NETWORKX_AVAILABLE = True
except ImportError:
    NETWORKX_AVAILABLE = False

logger = logging.getLogger(__name__)


class FraudGraphAnalyzer:
    """
    Analyse réseau de fraude — partage téléphones + comptes bancaires.
    Lazy loading : graphe construit à la première requête.
    Cache TTL : rebuild toutes les 5 minutes.
    """

    def __init__(self, cache_ttl_seconds: int = 300):
        self.enabled = NETWORKX_AVAILABLE
        self.G = None
        self.pagerank = {}
        self.last_build_time = 0.0
        self.cache_ttl = cache_ttl_seconds

        if not self.enabled:
            logger.warning("NetworkX non installé — graph_score = 0.0")

    def _build_graph(self):
        if not self.enabled:
            return

        now = time.time()
        if self.G is not None and (now - self.last_build_time) < self.cache_ttl:
            return

        try:
            from sqlalchemy import text
            from app.db.postgres import get_openg2p_db

            db     = get_openg2p_db()
            engine = db.engine

            phone_q = text("""
                SELECT partner_id,
                       COALESCE(NULLIF(phone_sanitized, ''), phone_no) AS phone
                FROM   g2p_phone_number
                WHERE  COALESCE(NULLIF(phone_sanitized, ''), phone_no) IS NOT NULL
            """)

            bank_q = text("""
                SELECT partner_id,
                       COALESCE(NULLIF(sanitized_acc_number, ''), acc_number) AS account
                FROM   res_partner_bank
                WHERE  active = true
                  AND  COALESCE(NULLIF(sanitized_acc_number, ''), acc_number) IS NOT NULL
            """)

            with engine.connect() as conn:
                phones = conn.execute(phone_q).fetchall()
                banks  = conn.execute(bank_q).fetchall()

            G = nx.Graph()

            for partner_id, phone in phones:
                G.add_edge(f"P{partner_id}", f"TEL:{phone}")

            for partner_id, account in banks:
                G.add_edge(f"P{partner_id}", f"BANK:{account}")

            self.G        = G
            self.pagerank = nx.pagerank(G, alpha=0.85, max_iter=100) if G.number_of_nodes() > 0 else {}
            self.last_build_time = now

            logger.info(f"Graphe : {G.number_of_nodes()} noeuds, {G.number_of_edges()} aretes")

        except Exception as e:
            logger.error(f"Erreur graphe : {e}")
            self.G        = nx.Graph() if NETWORKX_AVAILABLE else None
            self.pagerank = {}

    def get_risk(self, beneficiary_id: Any) -> Dict[str, Any]:
        default = {"graph_score": 0.0, "pagerank": 0.0, "degree": 0, "clustering": 0.0}

        if not self.enabled:
            return default

        self._build_graph()

        if self.G is None:
            return default

        node_id = f"P{beneficiary_id}"

        if node_id not in self.G:
            return default

        pr         = self.pagerank.get(node_id, 0.0)
        degree     = self.G.degree(node_id)
        clustering = nx.clustering(self.G, node_id)

        pr_norm     = min(pr * 5000, 1.0)
        degree_norm = min(degree / 20.0, 1.0)

        graph_score = round(0.50 * pr_norm + 0.30 * degree_norm + 0.20 * clustering, 4)

        return {
            "graph_score": graph_score,
            "pagerank":    round(pr, 6),
            "degree":      degree,
            "clustering":  round(clustering, 4),
        }

    def get_stats(self) -> Dict[str, Any]:
        if not self.enabled or self.G is None:
            return {"enabled": False, "nodes": 0, "edges": 0}
        return {
            "enabled":    True,
            "nodes":      self.G.number_of_nodes(),
            "edges":      self.G.number_of_edges(),
            "components": nx.number_connected_components(self.G),
        }