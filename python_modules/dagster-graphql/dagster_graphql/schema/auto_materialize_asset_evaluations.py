from typing import Optional, Tuple, Union, cast

import dagster._check as check
import graphene
from dagster import PartitionsDefinition
from dagster._core.definitions.auto_materialize_condition import (
    AutoMaterializeCondition,
    AutoMaterializeDecisionType,
    DownstreamFreshnessAutoMaterializeCondition,
    FreshnessAutoMaterializeCondition,
    MissingAutoMaterializeCondition,
    ParentMaterializedAutoMaterializeCondition,
    ParentOutdatedAutoMaterializeCondition,
)
from dagster._core.errors import DagsterError
from dagster._core.scheduler.instigation import AutoMaterializeAssetEvaluationRecord

from .util import non_null_list

GrapheneAutoMaterializeDecisionType = graphene.Enum.from_enum(AutoMaterializeDecisionType)


class GrapheneAutoMaterializeConditionWithDecisionType(graphene.Interface):
    decisionType = graphene.NonNull(GrapheneAutoMaterializeDecisionType)
    partitionKeys = graphene.List(graphene.NonNull(graphene.String))

    class Meta:
        name = "AutoMaterializeConditionWithDecisionType"


class GrapheneFreshnessAutoMaterializeCondition(graphene.ObjectType):
    class Meta:
        name = "FreshnessAutoMaterializeCondition"
        interfaces = (GrapheneAutoMaterializeConditionWithDecisionType,)


class GrapheneDownstreamFreshnessAutoMaterializeCondition(graphene.ObjectType):
    class Meta:
        name = "DownstreamFreshnessAutoMaterializeCondition"
        interfaces = (GrapheneAutoMaterializeConditionWithDecisionType,)


class GrapheneParentMaterializedAutoMaterializeCondition(graphene.ObjectType):
    class Meta:
        name = "ParentMaterializedAutoMaterializeCondition"
        interfaces = (GrapheneAutoMaterializeConditionWithDecisionType,)


class GrapheneMissingAutoMaterializeCondition(graphene.ObjectType):
    class Meta:
        name = "MissingAutoMaterializeCondition"
        interfaces = (GrapheneAutoMaterializeConditionWithDecisionType,)


class GrapheneParentOutdatedAutoMaterializeCondition(graphene.ObjectType):
    class Meta:
        name = "ParentOutdatedAutoMaterializeCondition"
        interfaces = (GrapheneAutoMaterializeConditionWithDecisionType,)


class GrapheneAutoMaterializeCondition(graphene.Union):
    class Meta:
        name = "AutoMaterializeCondition"
        types = (
            GrapheneFreshnessAutoMaterializeCondition,
            GrapheneDownstreamFreshnessAutoMaterializeCondition,
            GrapheneParentMaterializedAutoMaterializeCondition,
            GrapheneMissingAutoMaterializeCondition,
            GrapheneParentOutdatedAutoMaterializeCondition,
        )


DAGSTER_NAMEDTUPLE_TO_GRAPHENE_MAP = {
    FreshnessAutoMaterializeCondition: GrapheneFreshnessAutoMaterializeCondition,
    DownstreamFreshnessAutoMaterializeCondition: GrapheneDownstreamFreshnessAutoMaterializeCondition,
    ParentMaterializedAutoMaterializeCondition: GrapheneParentMaterializedAutoMaterializeCondition,
    MissingAutoMaterializeCondition: GrapheneMissingAutoMaterializeCondition,
    ParentOutdatedAutoMaterializeCondition: GrapheneParentOutdatedAutoMaterializeCondition,
}


def create_graphene_auto_materialize_condition(
    condition: Union[AutoMaterializeCondition, Tuple[AutoMaterializeCondition, str]],
    partitions_def: Optional[PartitionsDefinition],
):
    if partitions_def is not None:
        if not len(condition) == 2 and isinstance(condition[1], str):
            raise DagsterError(
                f"Unexpected condition type {type(condition)} for partitions definition"
                f" {partitions_def}"
            )
        auto_materialize_condition, serialized_subset = cast(
            Tuple[AutoMaterializeCondition, str], condition
        )
        subset = partitions_def.deserialize_subset(serialized_subset)
        partition_keys = subset.get_partition_keys()
    else:
        auto_materialize_condition = cast(AutoMaterializeCondition, condition)
        partition_keys = None

    check.invariant(
        type(auto_materialize_condition) in DAGSTER_NAMEDTUPLE_TO_GRAPHENE_MAP,
        f"Unexpected condition type {type(auto_materialize_condition)}",
    )

    graphene_type = DAGSTER_NAMEDTUPLE_TO_GRAPHENE_MAP[type(auto_materialize_condition)]
    return graphene_type(
        decisionType=auto_materialize_condition.decision_type, partitionKeys=partition_keys
    )


class GrapheneAutoMaterializeAssetEvaluationRecord(graphene.ObjectType):
    id = graphene.NonNull(graphene.ID)
    evaluationId = graphene.NonNull(graphene.Int)
    numRequested = graphene.NonNull(graphene.Int)
    numSkipped = graphene.NonNull(graphene.Int)
    numDiscarded = graphene.NonNull(graphene.Int)
    conditions = non_null_list(GrapheneAutoMaterializeCondition)

    class Meta:
        name = "AutoMaterializeAssetEvaluationRecord"

    def __init__(
        self,
        record: AutoMaterializeAssetEvaluationRecord,
        partitions_def: Optional[PartitionsDefinition],
    ):
        super().__init__(
            id=record.id,
            evaluationId=record.evaluation_id,
            numRequested=record.evaluation.num_requested,
            numSkipped=record.evaluation.num_skipped,
            numDiscarded=record.evaluation.num_discarded,
            conditions=[
                create_graphene_auto_materialize_condition(c, partitions_def)
                for c in record.evaluation.conditions
            ],
        )
