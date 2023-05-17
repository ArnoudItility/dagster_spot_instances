from typing import TYPE_CHECKING, Optional, Sequence

from dagster import AssetKey
from dagster._core.errors import DagsterError
from dagster._core.host_representation.external_data import ExternalAssetNode

from dagster_graphql.implementation.fetch_assets import asset_node_iter
from dagster_graphql.schema.auto_materialize_asset_evaluations import (
    GrapheneAutoMaterializeAssetEvaluationRecord,
)
from dagster_graphql.schema.inputs import GrapheneAssetKeyInput

if TYPE_CHECKING:
    from ..schema.util import ResolveInfo


def fetch_auto_materialize_asset_evaluations(
    graphene_info: "ResolveInfo",
    graphene_asset_key: GrapheneAssetKeyInput,
    limit: int,
    cursor: Optional[str],
) -> Sequence[GrapheneAutoMaterializeAssetEvaluationRecord]:
    """Fetch asset policy evaluations from storage."""
    asset_key = AssetKey.from_graphql_input(graphene_asset_key)

    asset_node: Optional[ExternalAssetNode] = None
    for _, _, external_asset_node in asset_node_iter(graphene_info):
        if external_asset_node.asset_key == asset_key:
            asset_node = external_asset_node
            break

    if not asset_node:
        raise DagsterError(f"Asset {asset_key} not found in workspace")

    partitions_def = partitions_def = (
        asset_node.partitions_def_data.get_partitions_definition()
        if asset_node.partitions_def_data
        else None
    )

    return [
        GrapheneAutoMaterializeAssetEvaluationRecord(record, partitions_def)
        for record in graphene_info.context.instance.schedule_storage.get_auto_materialize_asset_evaluations(
            asset_key=asset_key, limit=limit, cursor=int(cursor) if cursor else None
        )
    ]
