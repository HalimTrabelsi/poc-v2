"""
MLflow Experiment Dashboard — Visualize model training runs, metrics, and comparisons.

Streamlit app to explore MLflow experiments, compare runs, and visualize metrics.

Usage:
  streamlit run ml/dashboards/mlflow_dashboard.py
"""
import os
import json
from datetime import datetime
from typing import List, Dict, Any

import mlflow
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

st.set_page_config(
    page_title="MLflow Experiments",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Configure MLflow
MLFLOW_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
mlflow.set_tracking_uri(MLFLOW_URI)

st.title("🔍 MLflow Experiment Tracking")
st.markdown(f"Tracking server: **{MLFLOW_URI}**")

# ═══════════════════════════════════════════════════════════════════════════
# SIDEBAR: Experiment Selection
# ═══════════════════════════════════════════════════════════════════════════

with st.sidebar:
    st.header("Experiment Selection")

    # List all experiments
    experiments = mlflow.search_experiments()
    experiment_names = [exp.name for exp in experiments]

    selected_exp_name = st.selectbox(
        "Select Experiment",
        experiment_names,
        index=0 if experiment_names else None,
        key="experiment",
    )

    if selected_exp_name:
        selected_exp = next(e for e in experiments if e.name == selected_exp_name)
        st.write(f"**Experiment ID:** {selected_exp.experiment_id}")
        st.write(f"**Artifact Location:** {selected_exp.artifact_location}")

# ═══════════════════════════════════════════════════════════════════════════
# TAB 1: Experiment Runs
# ═══════════════════════════════════════════════════════════════════════════

tab1, tab2, tab3, tab4 = st.tabs(
    ["📈 Runs", "🔬 Comparison", "📊 Metrics Trend", "🎯 Dataset Info"]
)

with tab1:
    st.header("Experiment Runs")

    if selected_exp_name:
        experiment = next(e for e in experiments if e.name == selected_exp_name)

        # Retrieve runs
        runs = mlflow.search_runs(
            experiment_ids=[experiment.experiment_id],
            order_by=["start_time DESC"],
            max_results=50,
        )

        if not runs.empty:
            # Display runs table
            st.subheader("Recent Runs")
            runs_display = runs[[
                "run_id", "status", "start_time", "end_time", "duration"
            ]].copy()
            runs_display["start_time"] = pd.to_datetime(runs_display["start_time"])
            runs_display = runs_display.sort_values("start_time", ascending=False)

            st.dataframe(runs_display, use_container_width=True)

            # Run metrics
            st.subheader("Run Metrics Summary")
            metrics_df = runs[[col for col in runs.columns if col.startswith("metrics.")]].copy()
            if not metrics_df.empty:
                # Clean column names
                metrics_df.columns = [col.replace("metrics.", "") for col in metrics_df.columns]
                st.dataframe(metrics_df.head(10), use_container_width=True)

            # Run parameters
            st.subheader("Run Parameters")
            params_df = runs[[col for col in runs.columns if col.startswith("params.")]].copy()
            if not params_df.empty:
                params_df.columns = [col.replace("params.", "") for col in params_df.columns]
                st.dataframe(params_df.head(10), use_container_width=True)
        else:
            st.info("No runs found in this experiment.")
    else:
        st.warning("Please select an experiment.")

# ═══════════════════════════════════════════════════════════════════════════
# TAB 2: Run Comparison
# ═══════════════════════════════════════════════════════════════════════════

with tab2:
    st.header("Compare Runs")

    if selected_exp_name:
        experiment = next(e for e in experiments if e.name == selected_exp_name)
        runs = mlflow.search_runs(
            experiment_ids=[experiment.experiment_id],
            order_by=["start_time DESC"],
            max_results=50,
        )

        if not runs.empty:
            # Select runs to compare
            run_options = [f"{r['run_id'][:8]} ({r['tags.mlflow.runName']})"
                          for _, r in runs.iterrows()]
            selected_runs = st.multiselect(
                "Select runs to compare",
                range(len(run_options)),
                default=[0, 1] if len(run_options) > 1 else [0],
                format_func=lambda i: run_options[i],
            )

            if selected_runs:
                comparison_runs = runs.iloc[selected_runs]

                st.subheader("Metrics Comparison")
                metrics_cols = [col for col in comparison_runs.columns
                               if col.startswith("metrics.")]
                if metrics_cols:
                    # Rename for display
                    metrics_display = comparison_runs[metrics_cols].copy()
                    metrics_display.columns = [col.replace("metrics.", "") for col in metrics_cols]
                    metrics_display["run"] = [r[:8] for r in comparison_runs["run_id"]]
                    st.dataframe(metrics_display, use_container_width=True)

                    # Comparison chart
                    st.subheader("Metrics Radar Chart")
                    metrics_numeric = metrics_display.select_dtypes(include=["float64", "int64"])
                    if not metrics_numeric.empty:
                        # Create radar for first run
                        run_name = metrics_display["run"].iloc[0]
                        fig = go.Figure()
                        for idx, run_row in metrics_numeric.iterrows():
                            fig.add_trace(go.Scattergl(
                                x=metrics_numeric.columns,
                                y=run_row.values,
                                mode="lines+markers",
                                name=metrics_display["run"].iloc[idx],
                                fill="toself",
                            ))
                        st.plotly_chart(fig, use_container_width=True)

                st.subheader("Parameters Comparison")
                params_cols = [col for col in comparison_runs.columns
                              if col.startswith("params.")]
                if params_cols:
                    params_display = comparison_runs[params_cols].copy()
                    params_display.columns = [col.replace("params.", "") for col in params_cols]
                    params_display["run"] = [r[:8] for r in comparison_runs["run_id"]]
                    st.dataframe(params_display, use_container_width=True)
        else:
            st.info("No runs found in this experiment.")
    else:
        st.warning("Please select an experiment.")

# ═══════════════════════════════════════════════════════════════════════════
# TAB 3: Metrics Trends
# ═══════════════════════════════════════════════════════════════════════════

with tab3:
    st.header("Metrics Trends Over Time")

    if selected_exp_name:
        experiment = next(e for e in experiments if e.name == selected_exp_name)
        runs = mlflow.search_runs(
            experiment_ids=[experiment.experiment_id],
            order_by=["start_time DESC"],
            max_results=50,
        )

        if not runs.empty:
            # Get all metric columns
            metrics_cols = [col for col in runs.columns if col.startswith("metrics.")]

            if metrics_cols:
                selected_metric = st.selectbox(
                    "Select metric to visualize",
                    [col.replace("metrics.", "") for col in metrics_cols],
                )

                metric_col = f"metrics.{selected_metric}"
                if metric_col in runs.columns:
                    trend_data = runs[["start_time", metric_col]].copy()
                    trend_data.columns = ["start_time", "value"]
                    trend_data["start_time"] = pd.to_datetime(trend_data["start_time"])
                    trend_data = trend_data.sort_values("start_time")

                    fig = px.line(
                        trend_data,
                        x="start_time",
                        y="value",
                        title=f"{selected_metric} Trend",
                        labels={"start_time": "Time", "value": selected_metric},
                        markers=True,
                    )
                    st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No runs found in this experiment.")
    else:
        st.warning("Please select an experiment.")

# ═══════════════════════════════════════════════════════════════════════════
# TAB 4: Dataset Info
# ═══════════════════════════════════════════════════════════════════════════

with tab4:
    st.header("Dataset Information")

    if selected_exp_name:
        experiment = next(e for e in experiments if e.name == selected_exp_name)
        runs = mlflow.search_runs(
            experiment_ids=[experiment.experiment_id],
            order_by=["start_time DESC"],
            max_results=1,  # Get latest run
        )

        if not runs.empty:
            latest_run = runs.iloc[0]
            run_id = latest_run["run_id"]

            # Fetch artifact: dataset_info.json
            try:
                artifacts = mlflow.artifacts.download_artifacts(
                    run_id=run_id,
                    artifact_path="",
                    dst_path="/tmp/mlflow_artifacts",
                )
                dataset_info_path = f"/tmp/mlflow_artifacts/dataset_info.json"

                if os.path.exists(dataset_info_path):
                    with open(dataset_info_path, "r") as f:
                        dataset_info = json.load(f)

                    st.subheader("Dataset Summary")
                    col1, col2, col3, col4 = st.columns(4)
                    col1.metric("Total Rows", dataset_info.get("total_rows", "N/A"))
                    col2.metric("Features", dataset_info.get("n_features", "N/A"))
                    col3.metric("Fraud Rate", f"{dataset_info.get('fraud_rate_train', 0):.1%}")
                    col4.metric("Dataset", dataset_info.get("dataset_name", "unknown"))

                    st.subheader("Data Split")
                    split_data = {
                        "Train": dataset_info.get("train_rows", 0),
                        "Val": dataset_info.get("val_rows", 0),
                        "Test": dataset_info.get("test_rows", 0),
                    }
                    fig_split = px.pie(
                        values=split_data.values(),
                        names=split_data.keys(),
                        title="Train/Val/Test Split",
                    )
                    st.plotly_chart(fig_split, use_container_width=True)

                    # Feature stats
                    feature_stats_path = f"/tmp/mlflow_artifacts/feature_statistics.json"
                    if os.path.exists(feature_stats_path):
                        with open(feature_stats_path, "r") as f:
                            feature_stats = json.load(f)

                        st.subheader("Feature Statistics")
                        stats_df = pd.DataFrame(feature_stats).T
                        st.dataframe(stats_df, use_container_width=True)
                else:
                    st.info("Dataset info artifact not found in this run.")
            except Exception as e:
                st.error(f"Error loading dataset info: {e}")
        else:
            st.info("No runs found in this experiment.")
    else:
        st.warning("Please select an experiment.")

# Footer
st.markdown("---")
st.markdown("📚 **MLflow Integration** — Track and compare all model training runs")
st.markdown(f"Last refresh: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
