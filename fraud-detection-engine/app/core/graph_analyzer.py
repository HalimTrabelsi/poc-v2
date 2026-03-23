"""Graph Analyzer — NetworkX fraud network detection"""
import networkx as nx
import pandas as pd


class FraudGraphAnalyzer:
    def __init__(self):
        self.G = nx.Graph()

    def build_from_df(self, df: pd.DataFrame):
        self.G.clear()
        for _, row in df.iterrows():
            self.G.add_node(str(row["id"]), name=row.get("name", ""))

        for attr, weight in [
            ("phone",        0.9),
            ("address_hash", 0.7),
            ("agent_id",     0.4),
            ("bank_account", 1.0),
        ]:
            if attr in df.columns:
                self._link_by(df, attr, weight)

    def _link_by(self, df: pd.DataFrame, attr: str, weight: float):
        grouped = df.groupby(attr)["id"].apply(list)
        for _, ids in grouped.items():
            if len(ids) > 1:
                for i in range(len(ids)):
                    for j in range(i + 1, len(ids)):
                        a, b = str(ids[i]), str(ids[j])
                        if self.G.has_edge(a, b):
                            self.G[a][b]["weight"] = max(
                                self.G[a][b]["weight"], weight)
                        else:
                            self.G.add_edge(a, b, weight=weight)

    def get_risk(self, beneficiary_id: str) -> dict:
        bid = str(beneficiary_id)
        if bid not in self.G or len(self.G) < 2:
            return {
                "graph_score": 0.0,
                "connections": 0,
                "is_hub":      False,
                "neighbors":   [],
            }

        neighbors   = list(self.G.neighbors(bid))
        degree      = self.G.degree(bid)
        pr          = nx.pagerank(self.G, weight="weight")
        graph_score = min(pr.get(bid, 0) * 200, 1.0)

        return {
            "graph_score": round(graph_score, 3),
            "connections": len(neighbors),
            "is_hub":      degree > 5,
            "neighbors":   neighbors[:5],
        }