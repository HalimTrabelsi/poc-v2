"""Network analysis: Personalized PageRank + Louvain community detection.

Graph is built from shared-phone and shared-bank edges. Fraud scores of known
fraudsters (CRITICAL/HIGH cases already in the fraud DB) are injected as
PageRank personalization weights so a beneficiary connected to confirmed
fraudsters scores higher than one connected to clean beneficiaries.

Pipeline
--------
1. Fetch direct network edges for the target (RelationshipExtractor)
2. Expand to 2-hop ego-network (neighbors' mutual edges) for richer topology
3. Attach known fraud scores to each node (from FraudCaseRepository)
4. Personalized PageRank  — propagates fraud through weighted connections
5. Community detection    — flags if target sits inside a high-risk cluster
6. Aggregate into network_score
"""
import logging
from typing import Optional

logger = logging.getLogger(__name__)


class GraphAnalyzer:
    """Build and analyse a beneficiary's fraud network using PageRank + communities."""

    def __init__(self, relationship_extractor=None) -> None:
        from app.data.extractors import RelationshipExtractor
        self._extractor = relationship_extractor or RelationshipExtractor()

    # ── public API ───────────────────────────────────────────────────────────

    def analyze_network(
        self,
        beneficiary_id: str,
        known_fraud_scores: Optional[dict[str, float]] = None,
    ) -> dict:
        """Return network risk signals for a single beneficiary.

        Args:
            beneficiary_id: OpenG2P partner_id (as string).
            known_fraud_scores: {partner_id_str: final_score} for already-scored
                beneficiaries. Used as PageRank personalization seeds.

        Returns:
            Dict with network_score, pagerank_score, community_risk,
            network_size, density, fraud_connections, risk_factors.
        """
        try:
            edges = self._extractor.get_network_edges(beneficiary_id)
        except Exception as exc:
            logger.warning("Could not fetch edges for %s: %s", beneficiary_id, exc)
            edges = []

        empty = {
            "network_score": 0.0, "pagerank_score": 0.0, "community_risk": 0.0,
            "network_size": 0, "density": 0.0, "fraud_connections": 0,
            "risk_factors": [],
        }
        if not edges:
            return empty

        try:
            import networkx as nx
        except ImportError:
            logger.error("networkx not installed — graph analysis disabled")
            return empty

        # Expand to 2-hop ego-network
        expanded_edges = self._expand_ego_network(edges, beneficiary_id)

        G = self._build_graph(expanded_edges, beneficiary_id)
        seed_scores = known_fraud_scores or {}

        pagerank_score = self._personalized_pagerank(G, beneficiary_id, seed_scores, nx)
        community_risk = self._community_risk(G, beneficiary_id, seed_scores, nx)
        density        = float(nx.density(G))
        network_size   = G.number_of_nodes() - 1

        shared_account_edges = sum(1 for e in edges if e.get("edge_type") == "shared_account")
        shared_phone_edges   = sum(1 for e in edges if e.get("edge_type") == "shared_phone")

        risk_factors: list[str] = []
        if pagerank_score > 0.3:
            risk_factors.append(
                f"High fraud PageRank: {pagerank_score:.2f} — connected to known-fraud network"
            )
        if community_risk > 0.4:
            risk_factors.append(
                f"Fraud community risk: {community_risk:.2f} — sits in high-risk cluster"
            )
        if network_size >= 5:
            risk_factors.append(f"Large network: {network_size} connected beneficiaries")
        if shared_account_edges >= 2:
            risk_factors.append(f"Shared bank account with {shared_account_edges} beneficiaries")
        if shared_phone_edges >= 3:
            risk_factors.append(f"Shared phone with {shared_phone_edges} beneficiaries")
        if density > 0.5:
            risk_factors.append(f"Dense fraud cluster (density={density:.2f})")

        network_score = min(1.0, round(
            pagerank_score * 0.50
            + community_risk * 0.30
            + density * 0.20,
            4,
        ))

        return {
            "network_score": network_score,
            "pagerank_score": round(pagerank_score, 4),
            "community_risk": round(community_risk, 4),
            "network_size": network_size,
            "density": round(density, 4),
            "fraud_connections": shared_account_edges + shared_phone_edges,
            "risk_factors": risk_factors,
        }

    # ── graph construction ───────────────────────────────────────────────────

    def _build_graph(self, edges: list[dict], center_id: str):
        import networkx as nx
        G = nx.Graph()
        G.add_node(str(center_id))
        for edge in edges:
            src  = str(edge.get("source", ""))
            tgt  = str(edge.get("target", ""))
            w    = float(edge.get("weight", 1.0))
            etype = str(edge.get("edge_type", "unknown"))
            if src and tgt:
                G.add_edge(src, tgt, weight=w, edge_type=etype)
        return G

    def _expand_ego_network(self, direct_edges: list[dict], center_id: str) -> list[dict]:
        """Add mutual edges between direct neighbours (2-hop expansion).

        This enriches the subgraph so PageRank and community detection have
        enough topology to be meaningful even for small local networks.
        """
        neighbor_ids = set()
        for e in direct_edges:
            if str(e.get("source")) != str(center_id):
                neighbor_ids.add(str(e.get("source")))
            if str(e.get("target")) != str(center_id):
                neighbor_ids.add(str(e.get("target")))

        if len(neighbor_ids) < 2:
            return direct_edges  # no expansion possible

        try:
            expanded = list(direct_edges)
            # Query edges between neighbors (not starting from center)
            for nid in list(neighbor_ids)[:20]:   # cap at 20 neighbours for performance
                try:
                    neighbour_edges = self._extractor.get_network_edges(nid)
                    for e in neighbour_edges:
                        # Only include edges that stay within the known neighborhood
                        src, tgt = str(e.get("source")), str(e.get("target"))
                        if src in neighbor_ids and tgt in neighbor_ids:
                            expanded.append(e)
                except Exception:
                    pass
            return expanded
        except Exception as exc:
            logger.debug("Ego-network expansion failed: %s", exc)
            return direct_edges

    # ── PageRank ─────────────────────────────────────────────────────────────

    def _personalized_pagerank(
        self,
        G,
        center_id: str,
        seed_scores: dict[str, float],
        nx,
    ) -> float:
        """Run personalized PageRank seeded by known fraud scores.

        Nodes with confirmed fraud (CRITICAL/HIGH) act as teleport magnets.
        A beneficiary reachable from many fraudsters absorbs more PageRank mass.
        Returns the target node's normalized PageRank value in [0, 1].
        """
        if G.number_of_nodes() < 2:
            return 0.0

        try:
            # Personalization: seed vector weighted by known fraud scores
            # Nodes with no known score get a small baseline (0.05) so the
            # random walker doesn't get stuck on isolated fraud seeds.
            baseline = 0.05
            personalization: dict[str, float] = {}
            for node in G.nodes():
                score = seed_scores.get(str(node), 0.0)
                personalization[node] = max(baseline, score)

            # Normalize so values sum to 1 (required by nx.pagerank)
            total = sum(personalization.values()) or 1.0
            personalization = {k: v / total for k, v in personalization.items()}

            pr = nx.pagerank(
                G,
                alpha=0.85,
                personalization=personalization,
                weight="weight",
                max_iter=200,
            )

            raw_score = pr.get(str(center_id), 0.0)

            # Normalize by the maximum possible uniform score so values are
            # interpretable regardless of graph size
            uniform = 1.0 / G.number_of_nodes()
            # Score > 1.0 means the node attracts more than its fair share of walk
            normalized = min(1.0, raw_score / max(uniform, 1e-9))
            return float(normalized)

        except Exception as exc:
            logger.debug("PageRank failed: %s", exc)
            return 0.0

    # ── Community detection ───────────────────────────────────────────────────

    def _community_risk(
        self,
        G,
        center_id: str,
        seed_scores: dict[str, float],
        nx,
    ) -> float:
        """Return fraction of the target's community that are known fraudsters.

        Uses NetworkX label propagation (available in all recent versions,
        no extra dependencies). Falls back to Louvain if python-louvain is
        installed.
        """
        if G.number_of_nodes() < 3:
            return 0.0

        try:
            communities = self._detect_communities(G, nx)
            center = str(center_id)

            # Find which community the center belongs to
            target_community: set[str] = set()
            for comm in communities:
                if center in comm:
                    target_community = comm
                    break

            if not target_community or len(target_community) < 2:
                return 0.0

            # Fraction of community members with HIGH/CRITICAL known fraud score
            fraud_threshold = 0.60   # HIGH boundary
            n_fraudsters = sum(
                1 for node in target_community
                if seed_scores.get(str(node), 0.0) >= fraud_threshold
            )
            return float(n_fraudsters) / len(target_community)

        except Exception as exc:
            logger.debug("Community detection failed: %s", exc)
            return 0.0

    def _detect_communities(self, G, nx) -> list[set[str]]:
        """Return a list of node-sets, one per community."""
        # Try Louvain first (best quality, optional dep)
        try:
            from community import best_partition   # python-louvain package
            partition = best_partition(G)
            groups: dict[int, set] = {}
            for node, cid in partition.items():
                groups.setdefault(cid, set()).add(str(node))
            return list(groups.values())
        except ImportError:
            pass

        # Fall back to label propagation (always available in NetworkX >= 2.6)
        try:
            communities = nx.community.label_propagation_communities(G)
            return [set(map(str, c)) for c in communities]
        except Exception:
            pass

        # Last resort: each connected component is its own community
        return [set(map(str, c)) for c in nx.connected_components(G)]
