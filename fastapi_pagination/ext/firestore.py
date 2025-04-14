__all__ = [
    "apaginate",
    "paginate",
]

from collections.abc import Sequence
from functools import partial
from typing import Any, Optional, TypeVar, Union, cast

from google.cloud.firestore_v1 import (
    AsyncCollectionReference,
    AsyncQuery,
    AsyncTransaction,
    CollectionReference,
    DocumentSnapshot,
    Query,
    Transaction,
)
from google.cloud.firestore_v1.types import StructuredQuery
from typing import List
from google.cloud.firestore_v1.aggregation import AggregationQuery
from google.cloud.firestore_v1.async_aggregation import AsyncAggregationQuery
from typing_extensions import TypeAlias

from fastapi_pagination.bases import AbstractParams, CursorRawParams, RawParams
from fastapi_pagination.config import Config
from fastapi_pagination.ext.utils import generic_query_apply_params
from fastapi_pagination.flow import AnyFlow, Flow, flow, run_async_flow, run_sync_flow
from fastapi_pagination.flows import (
    CursorFlow,
    LimitOffsetFlow,
    TotalFlow,
    generic_flow,
)
from fastapi_pagination.types import (
    AdditionalData,
    ItemsTransformer,
    SyncItemsTransformer,
)

TQuery = TypeVar("TQuery", Query, AsyncQuery)

AnyQuery: TypeAlias = Union[Query, AsyncQuery]
AnyTransaction: TypeAlias = Union[Transaction, AsyncTransaction]


def _apply_cursor(
    query: TQuery,
    params: CursorRawParams,
    snapshot: Optional[DocumentSnapshot],
) -> TQuery:
    if snapshot is not None:
        query = query.start_after(snapshot)
    if params.size is not None:
        query = query.limit(params.size)
    return query


def _convert_raw_items(
    items: Sequence[DocumentSnapshot], /
) -> Sequence[dict[str, Any]]:
    return [(doc.to_dict() or {}) | {"id": str(doc.id)} for doc in items]


def _reverse_direction(
    direction: StructuredQuery.Direction,
) -> StructuredQuery.Direction:
    return (
        StructuredQuery.Direction.DESCENDING
        if direction == StructuredQuery.Direction.ASCENDING
        else StructuredQuery.Direction.ASCENDING
    )


def _build_previous_page_query_dynamic(
    current_query: Query,
    first_item_snapshot: DocumentSnapshot,
    page_size: Optional[int],
) -> Query:
    """
    Builds a previous-page query based on the internal ordering of an existing query.

    :param current_query: Firestore query used to get the current page (must already include ordering)
    :param first_item_snapshot: Snapshot of the first document in the current page
    :param page_size: Number of documents per page
    :return: Query object that will return the previous page (you must reverse the result list)
    """
    # Extract the collection from the query
    collection: CollectionReference = current_query._parent

    # Extract the filters from the original query
    filters = current_query._field_filters

    # Extract and reverse the order_by fields
    orders: List[StructuredQuery.Order] = current_query._orders or []
    reversed_orders = [
        StructuredQuery.Order(
            field=order.field,
            direction=_reverse_direction(order.direction),
        )
        for order in orders
    ]

    # Build new query from scratch
    prev_query = collection

    if filters:
        for f in filters:
            prev_query = prev_query.where(filter=f)

    for order in reversed_orders:
        prev_query = prev_query.order_by(
            order.field.field_path,
            direction=order.direction.name,  # "ASCENDING" or "DESCENDING"
        )

    # Apply end_at on the first document of the current page
    if first_item_snapshot is not None:
        prev_query = prev_query.end_at(first_item_snapshot)
    if page_size is not None:
        prev_query = prev_query.limit(page_size + 1)

    return prev_query


@flow
def _get_total(
    async_: bool,
    query: AnyQuery,
    transaction: Optional[AnyTransaction],
) -> Flow[Any, int]:
    aggr_query = AsyncAggregationQuery if async_ else AggregationQuery
    total_res = yield aggr_query(query).count("total").get(transaction=transaction)

    return cast(int, total_res[0][0].value)


@flow
def _total_flow(
    async_: bool,
    query: AnyQuery,
    transaction: Optional[AnyTransaction],
) -> TotalFlow:
    aggr_query = AsyncAggregationQuery if async_ else AggregationQuery
    total_res = yield aggr_query(query).count("total").get(transaction=transaction)

    return cast(int, total_res[0][0].value)


@flow
def _limit_offset_flow(
    query: AnyQuery,
    transaction: Optional[AnyTransaction],
    raw_params: RawParams,
) -> LimitOffsetFlow:
    query = generic_query_apply_params(query, raw_params)
    items = yield query.get(transaction=transaction)  # type: ignore[arg-type]

    return items


@flow
def _fetch_cursor(
    query: AnyQuery,
    params: CursorRawParams,
    transaction: Optional[AnyTransaction],
) -> Flow[Any, Optional[DocumentSnapshot]]:
    if cursor := params.cursor:
        raw_doc = yield query._parent.document(cursor).get(transaction=transaction)
        return cast(DocumentSnapshot, raw_doc)

    return None


@flow
def _cursor_flow(
    query: AnyQuery,
    transaction: Optional[AnyTransaction],
    raw_params: CursorRawParams,
) -> CursorFlow:
    meta = {}

    # Run current page query
    snapshot = yield from _fetch_cursor(query, raw_params, transaction)
    if snapshot:
        meta["current"] = snapshot.id

    query = _apply_cursor(query, raw_params, snapshot)  # type: ignore[type-var]
    items = yield query.get(transaction=transaction)  # type: ignore[arg-type]
    if items:
        meta["next_"] = items[-1].id

    # Run previous page query
    previous_page_query = _build_previous_page_query_dynamic(
        query, snapshot, raw_params.size
    )
    prev_items = yield previous_page_query.get(transaction=transaction)  # type: ignore[arg-type]
    if raw_params.size is not None and (len(prev_items) > raw_params.size):
        meta["previous"] = prev_items[0].id

    return items, meta


@flow
def _firebase_flow(
    src: Union[
        CollectionReference,
        Query,
        AsyncCollectionReference,
        AsyncQuery,
    ],
    /,
    params: Optional[AbstractParams],
    *,
    raw: bool,
    transaction: Optional[AnyTransaction],
    transformer: Optional[ItemsTransformer],
    additional_data: Optional[AdditionalData],
    config: Optional[Config],
    async_: bool,
) -> AnyFlow:
    query: AnyQuery
    if isinstance(src, AsyncCollectionReference):
        query = AsyncQuery(src)
    elif isinstance(src, CollectionReference):
        query = Query(src)
    else:
        query = src

    inner_transformer: Optional[ItemsTransformer] = None
    if not raw:
        inner_transformer = _convert_raw_items

    page = yield from generic_flow(
        async_=async_,
        total_flow=partial(_total_flow, async_, query, transaction),
        limit_offset_flow=partial(_limit_offset_flow, query, transaction),
        cursor_flow=partial(_cursor_flow, query, transaction),
        params=params,
        inner_transformer=inner_transformer,
        transformer=transformer,
        additional_data=additional_data,
        config=config,
    )

    return page


def paginate(
    src: Union[CollectionReference, Query],
    /,
    params: Optional[AbstractParams] = None,
    *,
    raw: bool = False,
    transaction: Optional[Transaction] = None,
    transformer: Optional[SyncItemsTransformer] = None,
    additional_data: Optional[AdditionalData] = None,
    config: Optional[Config] = None,
) -> Any:
    return run_sync_flow(
        _firebase_flow(
            src,
            params=params,
            raw=raw,
            transaction=transaction,
            transformer=transformer,
            additional_data=additional_data,
            config=config,
            async_=False,
        ),
    )


async def apaginate(
    src: Union[AsyncCollectionReference, AsyncQuery],
    /,
    params: Optional[AbstractParams] = None,
    *,
    raw: bool = False,
    transaction: Optional[AsyncTransaction] = None,
    transformer: Optional[ItemsTransformer] = None,
    additional_data: Optional[AdditionalData] = None,
    config: Optional[Config] = None,
) -> Any:
    return await run_async_flow(
        _firebase_flow(
            src,
            params=params,
            raw=raw,
            transaction=transaction,
            transformer=transformer,
            additional_data=additional_data,
            config=config,
            async_=True,
        ),
    )
