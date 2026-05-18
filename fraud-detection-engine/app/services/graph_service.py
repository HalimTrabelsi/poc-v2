"""Network analysis service using NetworkX."""
import logging
from typing import Optional

logger = logging.getLogger(__name__)


class GraphAnalyzer:
    """Build and analyse a beneficiary's fraud network."""

    def __init__(self, relationship_extractor=None) -> None:
        # Import here so the module is importable even without networkx installed
        from app.data.extractors import RelationshipExtractor

        self._extractor = relationship_extractor or RelationshipExtractor()

    def analyze_network(self, beneficiary_id: str) -> dict:
        """Build graph and return network signals.

        Returns:
            Dict with keys: network_score, network_size, density,
            fraud_connections, risk_factors.
        """
        try:
            edges = self._extractor.get_network_edges(beneficiary_id)
        except Exception as exc:
            logger.warning("Could not fetch edges for %s: %s", beneficiary_id, exc)
            edges = []

        if not edges:
            return {
                "network_score": 0.0,
                "network_size": 0,
                "density": 0.0,
                "fraud_connections": 0,
                "risk_factors": [],
            }

        graph = self._build_graph(edges, beneficiary_id)
        metrics = self._calculate_metrics(graph, beneficiary_id, edges)
        return metrics

    def _build_graph(self, edges: list[dict], center_id: str):
        """Return a weighted NetworkX Graph from edge dicts."""
        try:
            import networkx as nx
        except ImportError:
            logger.error("networkx is not installed; graph analysis disabled")
            return None

        G = nx.Graph()
        G.add_node(str(center_id))

        for edge in edges:
            source = str(edge.get("source", ""))
            target = str(edge.get("target", ""))
            weight = float(edge.get("weight", 1.0))
            edge_type = str(edge.get("edge_type", "unknown"))
            if source and target:
                G.add_edge(source, target, weight=weight, edge_type=edge_type)

        return G

    def _calculate_metrics(self, graph, center_id: str, edges: list[dict]) -> dict:
        """Derive fraud-relevant network metrics from the graph."""
        if graph is None:
            return {
                "network_score": 0.0,
                "network_size": 0,
                "density": 0.0,
                "fraud_connections": 0,
                "risk_factors": [],
            }

        try:
            import networkx as nx

            center = str(center_id)
            network_size = graph.number_of_nodes() - 1  # exclude self
            density = float(nx.density(graph))

            centrality = nx.degree_centrality(graph)
            center_centrality = centrality.get(center, 0.0)

            clustering = nx.clustering(graph)
            center_clustering = clustering.get(center, 0.0)

            shared_account_edges = sum(
                1 for e in edges if e.get("edge_type") == "shared_account"
            )
            shared_phone_edges = sum(
                1 for e in edges if e.get("edge_type") == "shared_phone"
            )

            risk_factors: list[str] = []
            if network_size >= 5:
                risk_factors.append(f"Large network: {network_size} connected beneficiaries")
            if shared_account_edges >= 2:
                risk_factors.append(f"Shared bank account with {shared_account_edges} beneficiaries")
            if shared_phone_edges >= 3:
                risk_factors.append(f"Shared phone with {shared_phone_edges} beneficiaries")
            if density > 0.5:
                risk_factors.append(f"Dense fraud cluster (density={density:.2f})")

            # Weighted score: centrality has high diagnostic value in fraud rings
            network_score = min(
                1.0,
                center_centrality * 0.40
                + density * 0.30
                + min(network_size / 10, 1.0) * 0.30,
            )

            return {
                "network_score": round(network_score, 4),
                "network_size": network_size,
                "density": round(density, 4),
                "fraud_connections": shared_account_edges + shared_phone_edges,
                "risk_factors": risk_factors,
            }
        except Exception as exc:
            logger.error("Graph metric calculation failed: %s", exc)
            return {
                "network_score": 0.0,
                "network_size": len(edges),
                "density": 0.0,
                "fraud_connections": len(edges),
                "risk_factors": [],
            }
