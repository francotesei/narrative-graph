"""Streamlit application for Narrative Graph Intelligence."""

import streamlit as st

st.set_page_config(
    page_title="Narrative Graph Intelligence",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        color: #1f2937;
        margin-bottom: 0.5rem;
    }
    .sub-header {
        font-size: 1.1rem;
        color: #6b7280;
        margin-bottom: 2rem;
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1.5rem;
        border-radius: 1rem;
        color: white;
    }
    .risk-high {
        background-color: #fee2e2;
        border-left: 4px solid #ef4444;
        padding: 1rem;
        border-radius: 0.5rem;
    }
    .risk-medium {
        background-color: #fef3c7;
        border-left: 4px solid #f59e0b;
        padding: 1rem;
        border-radius: 0.5rem;
    }
    .risk-low {
        background-color: #d1fae5;
        border-left: 4px solid #10b981;
        padding: 1rem;
        border-radius: 0.5rem;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 2rem;
    }
    .stTabs [data-baseweb="tab"] {
        padding: 1rem 2rem;
    }
</style>
""", unsafe_allow_html=True)


def main():
    """Main Streamlit application."""
    # Sidebar
    with st.sidebar:
        st.image("https://img.icons8.com/fluency/96/graph.png", width=60)
        st.title("Narrative Graph")
        st.markdown("---")

        # Run selector
        st.subheader("📁 Select Run")
        run_id = get_available_runs()

        st.markdown("---")

        # Navigation
        st.subheader("📍 Navigation")
        page = st.radio(
            "Go to",
            ["Overview", "Narrative Detail", "Coordination", "Graph View"],
            label_visibility="collapsed",
        )

        st.markdown("---")

        # Quick stats
        if run_id:
            show_quick_stats(run_id)

    # Main content
    if not run_id:
        show_welcome_page()
        return

    if page == "Overview":
        show_overview(run_id)
    elif page == "Narrative Detail":
        show_narrative_detail(run_id)
    elif page == "Coordination":
        show_coordination(run_id)
    elif page == "Graph View":
        show_graph_view(run_id)


def get_available_runs():
    """Get available runs from storage."""
    from narrative_graph.storage.database import RunDatabase

    try:
        db = RunDatabase()
        runs = db.list_runs(20)

        if not runs:
            st.warning("No runs found. Run the pipeline first.")
            return None

        run_options = {
            f"{r['run_id']} ({r['status']})": r['run_id']
            for r in runs
        }

        selected = st.selectbox(
            "Run",
            options=list(run_options.keys()),
            label_visibility="collapsed",
        )

        return run_options.get(selected)

    except Exception as e:
        st.error(f"Error loading runs: {e}")
        return None


def show_quick_stats(run_id: str):
    """Show quick statistics in sidebar."""
    from narrative_graph.storage.parquet import ParquetStorage

    storage = ParquetStorage()

    try:
        if storage.exists("narratives", run_id):
            narrative_count = storage.get_row_count("narratives", run_id)
            st.metric("Narratives", narrative_count)

        if storage.exists("clustered", run_id):
            post_count = storage.get_row_count("clustered", run_id)
            st.metric("Posts", post_count)

        if storage.exists("coordination_groups", run_id):
            group_count = storage.get_row_count("coordination_groups", run_id)
            st.metric("Coord. Groups", group_count)

    except Exception:
        pass


def show_welcome_page():
    """Show welcome page when no run is selected."""
    st.markdown('<p class="main-header">Welcome to Narrative Graph Intelligence</p>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">Analyze narratives, detect coordination, and assess risks in social media data.</p>', unsafe_allow_html=True)

    st.markdown("---")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown("### 🚀 Getting Started")
        st.markdown("""
        1. Start Neo4j: `docker-compose up -d`
        2. Initialize DB: `uv run narrative-graph db-init`
        3. Run pipeline: `uv run narrative-graph run-all data/sample.jsonl`
        """)

    with col2:
        st.markdown("### 📊 Features")
        st.markdown("""
        - **Narrative Detection**: Cluster posts into themes
        - **Coordination Analysis**: Detect suspicious patterns
        - **Risk Scoring**: Assess threat levels
        - **Graph Visualization**: Explore relationships
        """)

    with col3:
        st.markdown("### 📚 Documentation")
        st.markdown("""
        - [README](README.md)
        - [Configuration](configs/config.yaml)
        - [API Docs](/docs)
        """)


def show_overview(run_id: str):
    """Show overview page with narrative list."""
    from narrative_graph.storage.parquet import ParquetStorage
    from narrative_graph.ingestion.schemas import NarrativeRisk

    st.markdown('<p class="main-header">📊 Overview</p>', unsafe_allow_html=True)

    storage = ParquetStorage()

    # Load data
    try:
        narratives_df = storage.load_dataframe("narratives", run_id)
        risks_df = storage.load_dataframe("risks", run_id) if storage.exists("risks", run_id) else None

        if risks_df is not None:
            narratives_df = narratives_df.merge(
                risks_df[["narrative_id", "risk_score", "risk_level"]],
                left_on="id",
                right_on="narrative_id",
                how="left",
            )

    except Exception as e:
        st.error(f"Error loading data: {e}")
        return

    # Filters
    col1, col2, col3 = st.columns(3)

    with col1:
        risk_filter = st.multiselect(
            "Risk Level",
            options=["HIGH", "MEDIUM", "LOW"],
            default=["HIGH", "MEDIUM", "LOW"],
        )

    with col2:
        min_size = st.slider("Minimum Size", 1, 100, 5)

    with col3:
        sort_by = st.selectbox(
            "Sort By",
            options=["risk_score", "size", "author_count"],
            index=0,
        )

    # Apply filters
    filtered_df = narratives_df[narratives_df["size"] >= min_size]

    if "risk_level" in filtered_df.columns:
        filtered_df = filtered_df[filtered_df["risk_level"].isin(risk_filter)]
        filtered_df = filtered_df.sort_values(sort_by, ascending=False)

    # Summary metrics
    st.markdown("---")
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Total Narratives", len(filtered_df))

    with col2:
        if "risk_level" in filtered_df.columns:
            high_risk = len(filtered_df[filtered_df["risk_level"] == "HIGH"])
            st.metric("High Risk", high_risk)

    with col3:
        total_posts = filtered_df["size"].sum()
        st.metric("Total Posts", int(total_posts))

    with col4:
        total_authors = filtered_df["author_count"].sum()
        st.metric("Total Authors", int(total_authors))

    # Narrative list
    st.markdown("---")
    st.subheader("Narratives")

    for _, row in filtered_df.head(20).iterrows():
        risk_class = f"risk-{row.get('risk_level', 'low').lower()}"

        with st.container():
            st.markdown(f"""
            <div class="{risk_class}">
                <strong>{row['id']}</strong> | 
                Size: {row['size']} | 
                Authors: {row['author_count']} | 
                Risk: {row.get('risk_score', 0):.2f} ({row.get('risk_level', 'N/A')})
                <br>
                <small>Keywords: {', '.join(row.get('keywords', [])[:5])}</small>
            </div>
            """, unsafe_allow_html=True)
            st.markdown("")


def show_narrative_detail(run_id: str):
    """Show detailed view of a narrative."""
    from narrative_graph.storage.parquet import ParquetStorage
    import plotly.express as px

    st.markdown('<p class="main-header">🔎 Narrative Detail</p>', unsafe_allow_html=True)

    storage = ParquetStorage()

    try:
        narratives_df = storage.load_dataframe("narratives", run_id)
        narrative_ids = narratives_df["id"].tolist()

    except Exception as e:
        st.error(f"Error loading narratives: {e}")
        return

    # Narrative selector
    selected_narrative = st.selectbox("Select Narrative", options=narrative_ids)

    if not selected_narrative:
        return

    # Load narrative data
    narrative = narratives_df[narratives_df["id"] == selected_narrative].iloc[0]

    # Load risk and explanation
    risks_df = None
    explanation = None

    if storage.exists("risks", run_id):
        risks_df = storage.load_dataframe("risks", run_id)
        risk_row = risks_df[risks_df["narrative_id"] == selected_narrative]
        if not risk_row.empty:
            risk = risk_row.iloc[0]

    if storage.exists("explanations", run_id):
        explanations_df = storage.load_dataframe("explanations", run_id)
        exp_row = explanations_df[explanations_df["target_id"] == selected_narrative]
        if not exp_row.empty:
            explanation = exp_row.iloc[0]["explanation_text"]

    # Display
    col1, col2 = st.columns([2, 1])

    with col1:
        st.subheader("Summary")

        if explanation:
            st.markdown(explanation)
        else:
            st.info("No explanation available. Run the explain command.")

    with col2:
        st.subheader("Metrics")
        st.metric("Size", narrative["size"])
        st.metric("Authors", narrative["author_count"])

        if risks_df is not None and not risk_row.empty:
            risk_level = risk["risk_level"]
            risk_color = {"HIGH": "🔴", "MEDIUM": "🟡", "LOW": "🟢"}.get(risk_level, "⚪")
            st.metric("Risk Score", f"{risk['risk_score']:.2f} {risk_color}")

    st.markdown("---")

    # Keywords and indicators
    col1, col2, col3 = st.columns(3)

    with col1:
        st.subheader("Keywords")
        keywords = narrative.get("keywords", [])
        if keywords is not None and len(keywords) > 0:
            for kw in list(keywords)[:10]:
                st.markdown(f"• {kw}")

    with col2:
        st.subheader("Top Domains")
        domains = narrative.get("top_domains", [])
        if domains is not None and len(domains) > 0:
            for d in list(domains)[:10]:
                st.markdown(f"• {d}")

    with col3:
        st.subheader("Top Hashtags")
        hashtags = narrative.get("top_hashtags", [])
        if hashtags is not None and len(hashtags) > 0:
            for h in list(hashtags)[:10]:
                st.markdown(f"• #{h}")

    # Posts in narrative
    st.markdown("---")
    st.subheader("Sample Posts")

    try:
        posts_df = storage.load_dataframe("clustered", run_id)
        narrative_posts = posts_df[posts_df["narrative_id"] == selected_narrative].head(10)

        for _, post in narrative_posts.iterrows():
            with st.expander(f"Post {post['id']} by {post.get('author_handle', post['author_id'])}"):
                st.markdown(post["text"])
                st.caption(f"Platform: {post['platform']} | Time: {post['timestamp']}")

    except Exception as e:
        st.warning(f"Could not load posts: {e}")


def show_coordination(run_id: str):
    """Show coordination analysis page."""
    from narrative_graph.storage.parquet import ParquetStorage

    st.markdown('<p class="main-header">🔗 Coordination Analysis</p>', unsafe_allow_html=True)

    storage = ParquetStorage()

    if not storage.exists("coordination_groups", run_id):
        st.warning("No coordination data available. Run detect-coordination first.")
        return

    try:
        groups_df = storage.load_dataframe("coordination_groups", run_id)
        pairs_df = storage.load_dataframe("coordination_pairs", run_id) if storage.exists("coordination_pairs", run_id) else None

    except Exception as e:
        st.error(f"Error loading coordination data: {e}")
        return

    # Summary
    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("Coordination Groups", len(groups_df))

    with col2:
        if pairs_df is not None:
            st.metric("Coordinated Pairs", len(pairs_df))

    with col3:
        total_accounts = groups_df["size"].sum()
        st.metric("Coordinated Accounts", int(total_accounts))

    st.markdown("---")

    # Groups list
    st.subheader("Coordination Groups")

    groups_df = groups_df.sort_values("score", ascending=False)

    for _, group in groups_df.head(10).iterrows():
        score = group["score"]
        score_color = "🔴" if score > 0.7 else "🟡" if score > 0.4 else "🟢"

        with st.expander(f"{score_color} {group['id']} - {group['size']} accounts (score: {score:.2f})"):
            st.markdown(f"**Evidence:** {group.get('evidence_summary', 'N/A')}")

            authors = group.get("author_ids", [])
            if authors is not None and len(authors) > 0:
                authors_list = list(authors)
                st.markdown("**Accounts:**")
                for author in authors_list[:10]:
                    st.markdown(f"• {author}")
                if len(authors_list) > 10:
                    st.markdown(f"*... and {len(authors_list) - 10} more*")

            narratives = group.get("narrative_ids", [])
            if narratives is not None and len(narratives) > 0:
                narratives_list = list(narratives)
                st.markdown(f"**Related Narratives:** {', '.join(narratives_list)}")


def show_graph_view(run_id: str):
    """Show graph visualization page."""
    from narrative_graph.storage.parquet import ParquetStorage
    from narrative_graph.graph.export import GraphExporter
    from narrative_graph.graph.connection import get_neo4j_connection
    import json

    st.markdown('<p class="main-header">🕸️ Graph View</p>', unsafe_allow_html=True)

    storage = ParquetStorage()

    try:
        narratives_df = storage.load_dataframe("narratives", run_id)
        narrative_ids = narratives_df["id"].tolist()

    except Exception as e:
        st.error(f"Error loading narratives: {e}")
        return

    # Narrative selector
    selected_narrative = st.selectbox(
        "Select Narrative to Visualize",
        options=narrative_ids,
    )

    if not selected_narrative:
        return

    # Export and visualize
    try:
        conn = get_neo4j_connection()
        if not conn.verify_connectivity():
            st.error("Cannot connect to Neo4j. Make sure it's running.")
            return

        exporter = GraphExporter(conn)
        graph_data = exporter.export_narrative_subgraph(selected_narrative)

        st.info(f"Graph has {graph_data['node_count']} nodes and {graph_data['edge_count']} edges")

        # Simple visualization using pyvis
        if graph_data["nodes"]:
            from pyvis.network import Network
            import tempfile

            net = Network(height="600px", width="100%", bgcolor="#ffffff", font_color="#333333")
            net.barnes_hut()

            # Add nodes
            for node in graph_data["nodes"][:100]:  # Limit for performance
                labels = node.get("labels", [])
                label = labels[0] if labels else "Unknown"
                props = node.get("properties", {})

                # Color by type
                color_map = {
                    "Author": "#3b82f6",
                    "Post": "#10b981",
                    "Narrative": "#8b5cf6",
                    "Domain": "#f59e0b",
                    "Hashtag": "#ef4444",
                    "Entity": "#6366f1",
                }

                net.add_node(
                    node["id"],
                    label=props.get("id", props.get("name", str(node["id"])))[:20],
                    color=color_map.get(label, "#9ca3af"),
                    title=f"{label}: {json.dumps(props, default=str)[:100]}",
                )

            # Add edges
            for edge in graph_data["edges"][:200]:  # Limit for performance
                net.add_edge(
                    edge["source"],
                    edge["target"],
                    title=edge.get("type", ""),
                )

            # Save and display
            with tempfile.NamedTemporaryFile(delete=False, suffix=".html") as f:
                net.save_graph(f.name)
                with open(f.name, "r") as html_file:
                    html_content = html_file.read()
                    st.components.v1.html(html_content, height=620)

        else:
            st.warning("No graph data available for this narrative.")

    except Exception as e:
        st.error(f"Error generating graph: {e}")
        st.info("Make sure you've run the build-graph command.")


if __name__ == "__main__":
    main()
