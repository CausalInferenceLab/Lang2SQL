"""Shared semantic governance predicates used by planner and compiler."""

from __future__ import annotations

from .catalog import (
    DimensionDisclosureTier,
    DimensionReviewPolicy,
    DimensionSpec,
    SemanticCatalog,
)


def dimension_is_released(catalog: SemanticCatalog, dimension: DimensionSpec) -> bool:
    if dimension.review_policy == DimensionReviewPolicy.AUTO_SAFE:
        return bool(
            dimension.raw_output_allowed
            and dimension.classification_policy_version
            == catalog.classification_policy_version
        )
    return bool(
        dimension.review_policy == DimensionReviewPolicy.RELEASE_REQUIRED
        and dimension.raw_output_allowed
        and dimension.disclosure_tier
        in {
            DimensionDisclosureTier.CONTROLLED_GROUPED,
            DimensionDisclosureTier.PUBLIC_GROUPED,
        }
        and (
            dimension.disclosure_tier != DimensionDisclosureTier.PUBLIC_GROUPED
            or (
                catalog.public_data_scope
                and catalog.public_data_fingerprint == catalog.fingerprint
            )
        )
        and dimension.release_reviewer
        and dimension.release_catalog_fingerprint == catalog.fingerprint
        and dimension.classification_policy_version
        == catalog.classification_policy_version
    )


def public_data_scope_confirmed(catalog: SemanticCatalog) -> bool:
    """Bind a public-data assertion to the exact active physical catalog."""

    return bool(
        catalog.public_data_scope
        and catalog.public_data_fingerprint == catalog.fingerprint
        and catalog.public_data_reviewer
    )


def predicate_dimension_is_selectable(
    catalog: SemanticCatalog, dimension: DimensionSpec
) -> bool:
    """Require the same public policy for every row-narrowing candidate UI."""

    return bool(
        public_data_scope_confirmed(catalog)
        and dimension_is_released(catalog, dimension)
        and dimension.disclosure_tier == DimensionDisclosureTier.PUBLIC_GROUPED
    )


def has_controlled_dimension(dimensions: list[DimensionSpec]) -> bool:
    return any(
        dimension.review_policy == DimensionReviewPolicy.RELEASE_REQUIRED
        and dimension.disclosure_tier == DimensionDisclosureTier.CONTROLLED_GROUPED
        for dimension in dimensions
    )
