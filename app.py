import streamlit as st
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState
import pandas as pd
import time
import base64

st.set_page_config(
    page_title="Unity Catalog Explorer",
    page_icon="🔍",
    layout="wide"
)

st.title("Unity Catalog テーブル検索")


@st.cache_resource
def get_workspace_client():
    """Databricks WorkspaceClientを初期化"""
    return WorkspaceClient()


@st.cache_data(ttl=300)
def get_warehouses():
    """SQLウェアハウス一覧を取得"""
    w = get_workspace_client()
    warehouses = list(w.warehouses.list())
    return [(wh.id, wh.name) for wh in warehouses]


def get_sample_data(warehouse_id: str, table_name: str, limit: int = 100, exclude_binary: bool = True):
    """テーブルのサンプルデータを取得"""
    from databricks.sdk.service.sql import Disposition, Format
    import requests
    import pyarrow.ipc as ipc
    import io

    w = get_workspace_client()

    # テーブル名をバッククォートで囲む（ハイフンや数字を含む名前に対応）
    parts = table_name.split(".")
    escaped_name = ".".join([f"`{part}`" for part in parts])

    # BINARYカラム（画像データ）を除外するかどうか
    if exclude_binary:
        # まずテーブル情報を取得してBINARYカラムを特定
        table_info = w.tables.get(full_name=table_name)
        if table_info.columns:
            non_binary_cols = []
            for col in table_info.columns:
                col_type = col.type_text.upper() if col.type_text else ""
                if "BINARY" not in col_type:
                    non_binary_cols.append(f"`{col.name}`")
            if non_binary_cols:
                columns_str = ", ".join(non_binary_cols)
                query = f"SELECT {columns_str} FROM {escaped_name} LIMIT {limit}"
            else:
                query = f"SELECT * FROM {escaped_name} LIMIT {limit}"
        else:
            query = f"SELECT * FROM {escaped_name} LIMIT {limit}"
    else:
        query = f"SELECT * FROM {escaped_name} LIMIT {limit}"

    statement = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=query,
        wait_timeout="30s",
        disposition=Disposition.EXTERNAL_LINKS,
        format=Format.ARROW_STREAM
    )

    # 結果を待機
    while statement.status.state in [StatementState.PENDING, StatementState.RUNNING]:
        time.sleep(0.5)
        statement = w.statement_execution.get_statement(statement.statement_id)

    if statement.status.state != StatementState.SUCCEEDED:
        raise Exception(f"クエリ失敗: {statement.status.error.message if statement.status.error else 'Unknown error'}")

    # EXTERNAL_LINKSの場合、Arrow形式でデータを取得
    if statement.result and statement.result.external_links:
        all_tables = []
        for link in statement.result.external_links:
            response = requests.get(link.external_link)
            if response.status_code == 200:
                # Arrow IPC形式でパース
                reader = ipc.open_stream(io.BytesIO(response.content))
                table = reader.read_all()
                all_tables.append(table)

        if all_tables:
            import pyarrow as pa
            combined = pa.concat_tables(all_tables)
            df = combined.to_pandas()
            # BINARYカラム（画像データ）をBase64に変換
            for col in df.columns:
                if df[col].dtype == object:
                    def convert_value(x):
                        if x is None:
                            return None
                        if isinstance(x, bytes):
                            return base64.b64encode(x).decode('utf-8')
                        if isinstance(x, dict):
                            # 構造体の場合、そのまま文字列化
                            return str(x)
                        return x
                    df[col] = df[col].apply(convert_value)
            return df

    # INLINEの場合（フォールバック）
    if statement.result and statement.result.data_array:
        columns = [col.name for col in statement.manifest.schema.columns]
        return pd.DataFrame(statement.result.data_array, columns=columns)

    return pd.DataFrame()


# デフォルト設定
DEFAULT_CATALOG = "samples"
DEFAULT_SCHEMA = "nyctaxi"


@st.cache_data(ttl=300)
def get_catalogs():
    """カタログ一覧を取得"""
    w = get_workspace_client()
    catalogs = list(w.catalogs.list())
    return [c.name for c in catalogs]


@st.cache_data(ttl=300)
def get_schemas(catalog_name: str):
    """指定カタログのスキーマ一覧を取得"""
    w = get_workspace_client()
    schemas = list(w.schemas.list(catalog_name=catalog_name))
    return [s.name for s in schemas]


@st.cache_data(ttl=300)
def get_tables(catalog_name: str, schema_name: str):
    """指定スキーマのテーブル一覧を取得"""
    w = get_workspace_client()
    tables = list(w.tables.list(catalog_name=catalog_name, schema_name=schema_name))
    return tables

@st.cache_data(ttl=300)
def get_table_lineage(table_full_name: str):
    """
    Unity Catalog table の 1-hop lineage（upstream / downstream）を取得
    input/output format: unchanged
    """
    w = get_workspace_client()

    endpoint = "/api/2.0/lineage-tracking/table-lineage"
    payload = {
        "table_name": table_full_name,
        "include_entity_lineage": True,  # 公式例に合わせる
    }

    def to_full_name(node: dict) -> str | None:
        ti = (node or {}).get("tableInfo")
        if not ti or ti.get("table_type") != "TABLE":
            return None
        return f"{ti['catalog_name']}.{ti['schema_name']}.{ti['name']}"

    try:
        # ポイント：query ではなく body(JSON) で渡す（公式例） [oai_citation:2‡Databricks Documentation](https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-lineage)
        resp = w.api_client.do(method="GET", path=endpoint, body=payload)

        upstream = sorted({to_full_name(n) for n in resp.get("upstreams", []) if to_full_name(n)})
        downstream = sorted({to_full_name(n) for n in resp.get("downstreams", []) if to_full_name(n)})

        return {"upstream": upstream, "downstream": downstream, "raw": resp, "endpoint": endpoint, "params": payload}

    except Exception as e:
        return {"upstream": [], "downstream": [], "error": str(e), "raw": None, "endpoint": endpoint, "params": payload}


@st.cache_data(ttl=60)
def get_table_info(full_name: str):
    """テーブルの詳細情報を取得"""
    w = get_workspace_client()
    return w.tables.get(full_name=full_name)


def search_tables(tables, search_term: str):
    """テーブル名で検索"""
    if not search_term:
        return tables
    search_lower = search_term.lower()
    return [t for t in tables if search_lower in t.name.lower()]


# サイドバー: カタログとスキーマの選択
with st.sidebar:
    st.header("フィルター設定")

    try:
        # SQLウェアハウス選択
        warehouses = get_warehouses()
        if warehouses:
            selected_warehouse = st.selectbox(
                "SQLウェアハウスを選択",
                options=warehouses,
                format_func=lambda x: x[1],
                index=0
            )
            warehouse_id = selected_warehouse[0] if selected_warehouse else None
        else:
            st.warning("利用可能なSQLウェアハウスがありません")
            warehouse_id = None

        st.divider()

        catalogs = get_catalogs()
        # デフォルトカタログのインデックスを取得
        default_cat_idx = catalogs.index(DEFAULT_CATALOG) if DEFAULT_CATALOG in catalogs else 0
        selected_catalog = st.selectbox(
            "カタログを選択",
            options=catalogs,
            index=default_cat_idx if catalogs else None
        )

        if selected_catalog:
            schemas = get_schemas(selected_catalog)
            # デフォルトスキーマのインデックスを取得
            default_schema_idx = schemas.index(DEFAULT_SCHEMA) if DEFAULT_SCHEMA in schemas else 0
            selected_schema = st.selectbox(
                "スキーマを選択",
                options=schemas,
                index=default_schema_idx if schemas else None
            )
        else:
            selected_schema = None

    except Exception as e:
        st.error(f"接続エラー: {str(e)}")
        selected_catalog = None
        selected_schema = None
        warehouse_id = None

# メインエリア
if selected_catalog and selected_schema:
    # 検索ボックス
    search_term = st.text_input(
        "テーブル名で検索",
        placeholder="テーブル名を入力...",
        key="search_input"
    )

    try:
        tables = get_tables(selected_catalog, selected_schema)
        filtered_tables = search_tables(tables, search_term)

        st.subheader(f"テーブル一覧 ({len(filtered_tables)}件)")

        if filtered_tables:
            # テーブル一覧をDataFrameで表示
            table_data = []
            for t in filtered_tables:
                table_type = t.table_type.value if t.table_type else "UNKNOWN"
                table_data.append({
                    "テーブル名": t.name,
                    "タイプ": table_type,
                    "コメント": t.comment or "",
                    "フルネーム": t.full_name
                })

            df = pd.DataFrame(table_data)

            # テーブル選択
            selected_table = st.selectbox(
                "詳細を表示するテーブルを選択",
                options=[t.full_name for t in filtered_tables],
                format_func=lambda x: x.split(".")[-1]
            )

            # テーブル一覧表示
            st.dataframe(
                df[["テーブル名", "タイプ", "コメント"]],
                use_container_width=True,
                hide_index=True
            )

            # 選択されたテーブルの詳細
            if selected_table:
                st.divider()
                st.subheader(f"テーブル詳細: {selected_table}")

                table_info = get_table_info(selected_table)

                col1, col2 = st.columns(2)

                with col1:
                    st.markdown("**基本情報**")
                    st.write(f"- フルネーム: `{table_info.full_name}`")
                    st.write(f"- タイプ: {table_info.table_type.value if table_info.table_type else 'N/A'}")
                    st.write(f"- データソース形式: {table_info.data_source_format.value if table_info.data_source_format else 'N/A'}")
                    if table_info.storage_location:
                        st.write(f"- ストレージ: `{table_info.storage_location}`")

                with col2:
                    st.markdown("**メタデータ**")
                    st.write(f"- 作成者: {table_info.created_by or 'N/A'}")
                    st.write(f"- 更新者: {table_info.updated_by or 'N/A'}")
                    if table_info.comment:
                        st.write(f"- コメント: {table_info.comment}")

                # カラム情報
                if table_info.columns:
                    st.markdown("**カラム一覧**")
                    columns_data = []
                    for col in table_info.columns:
                        columns_data.append({
                            "カラム名": col.name,
                            "データ型": col.type_text,
                            "Nullable": "Yes" if col.nullable else "No",
                            "コメント": col.comment or ""
                        })

                    columns_df = pd.DataFrame(columns_data)
                    st.dataframe(columns_df, use_container_width=True, hide_index=True)

                # Lineage表示
                st.divider()
                st.markdown("**テーブルLineage**")
                lineage = get_table_lineage(selected_table)
                if lineage.get("error"):
                    st.error(f"Lineage取得に失敗しました: {lineage['error']}")
                else:
                    col_u, col_d = st.columns(2)
                    with col_u:
                        st.markdown("**上流テーブル (Upstream)**")
                        if lineage["upstream"]:
                            up_df = pd.DataFrame({"テーブル名": lineage["upstream"]})
                            st.dataframe(up_df, use_container_width=True, hide_index=True)
                        else:
                            st.write("なし")
                    with col_d:
                        st.markdown("**下流テーブル (Downstream)**")
                        if lineage["downstream"]:
                            down_df = pd.DataFrame({"テーブル名": lineage["downstream"]})
                            st.dataframe(down_df, use_container_width=True, hide_index=True)
                        else:
                            st.write("なし")
                    if not lineage["upstream"] and not lineage["downstream"]:
                        with st.expander("Lineageレスポンスの詳細"):
                            st.write("- エンドポイント:", lineage.get("endpoint") or "不明")
                            st.write("- パラメータ:", lineage.get("params") or {})
                            st.write("- 生レスポンス:")
                            st.write(lineage.get("raw") or {})

                # サンプルデータ表示
                st.divider()
                st.markdown("**サンプルデータ**")

                if warehouse_id:
                    sample_limit = st.slider("表示行数", min_value=10, max_value=500, value=10, step=10)

                    if st.button("サンプルデータを取得", type="primary"):
                        with st.spinner("データを取得中..."):
                            try:
                                sample_df = get_sample_data(warehouse_id, selected_table, sample_limit, exclude_binary=True)
                                if not sample_df.empty:
                                    st.dataframe(sample_df, use_container_width=True, hide_index=True)
                                    st.caption(f"{len(sample_df)}行を表示")
                                else:
                                    st.info("データがありません")
                            except Exception as e:
                                st.error(f"サンプルデータの取得に失敗しました: {str(e)}")
                else:
                    st.warning("サンプルデータを表示するにはSQLウェアハウスを選択してください")
        else:
            st.info("該当するテーブルが見つかりませんでした。")

    except Exception as e:
        st.error(f"テーブル情報の取得に失敗しました: {str(e)}")

else:
    st.info("サイドバーからカタログとスキーマを選択してください。")

# フッター
st.divider()
st.caption("Unity Catalog Explorer - Powered by Databricks Apps")