#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import pytest
from graphql_query import Argument, Field, Operation, Query
from source_shopify.shopify_graphql.bulk.query import MetafieldCustomer, MetafieldProductImage, ShopifyBulkQuery, ShopifyBulkTemplates


def test_query_status():
    expected = """query {
                    node(id: "gid://shopify/BulkOperation/4047052112061") {
                        ... on BulkOperation {
                            id
                            status
                            errorCode
                            objectCount
                            fileSize
                            url
                            partialDataUrl
                        }
                    }
                }"""
    
    input_job_id = "gid://shopify/BulkOperation/4047052112061"
    template = ShopifyBulkTemplates.status(input_job_id)
    assert repr(template) == repr(expected)
    
    
def test_bulk_query_prepare():
    expected = '''mutation {
                bulkOperationRunQuery(
                    query: """
                    {some_query}
                    """
                ) {
                    bulkOperation {
                        id
                        status
                    }
                    userErrors {
                        field
                        message
                    }
                }
            }'''
    
    input_query_from_slice = "{some_query}"
    template = ShopifyBulkTemplates.prepare(input_query_from_slice)
    assert repr(template) == repr(expected)
    

@pytest.mark.parametrize(
    "query_name, fields, filter_field, start, end, expected",
    [
        (
            "test_root", 
            ["test_field1", "test_field2"], 
            "updated_at",
            "2023-01-01",
            "2023-01-02", 
            Query(
                name='test_root', 
                arguments=[
                    Argument(name="query", value=f"\"updated_at:>'2023-01-01' AND updated_at:<='2023-01-02'\""), 
                ], 
                fields=[Field(name='edges', fields=[Field(name='node', fields=["test_field1", "test_field2"])])]
            )
        )
    ],
    ids=["simple query with filter and sort"]
)
def test_base_build_query(query_name, fields, filter_field, start, end, expected):
    """
    Expected result rendered:
    '''
    {
        test_root(query: "updated_at:>'2023-01-01' AND updated_at:<='2023-01-02'") {
            edges {
                node {
                id
                test_field1
                test_field2
            }
        }
    }
    '''
    """
    
    
    builder = ShopifyBulkQuery()
    filter_query = f"{filter_field}:>'{start}' AND {filter_field}:<='{end}'"
    built_query = builder.build(query_name, fields, filter_query)
    assert expected.render() == built_query.render()


@pytest.mark.parametrize(
    "query_class, filter_field, start, end, expected",
    [
        (
            MetafieldCustomer,
            "updated_at",
            "2023-01-01",
            "2023-01-02",
            Operation(
                type="",
                queries=[
                    Query(
                        name='customers', 
                        arguments=[
                            Argument(name="query", value=f"\"updated_at:>='2023-01-01' AND updated_at:<='2023-01-02'\""),
                            Argument(name="sortKey", value="UPDATED_AT"),    
                        ], 
                        fields=[Field(name='edges', fields=[Field(name='node', fields=['id', Field(name="metafields", fields=[Field(name="edges", fields=[Field(name="node", fields=["id", "namespace", "value", "key", "description", "createdAt", "updatedAt", "type"])])])])])]
                    )
                ]
            ),
        ),
        (
            MetafieldProductImage,
            "updated_at",
            "2023-01-01",
            "2023-01-02",
            Operation(
                type="",
                queries=[
                    Query(
                        name='products', 
                        arguments=[
                            Argument(name="query", value=f"\"updated_at:>='2023-01-01' AND updated_at:<='2023-01-02'\""),
                            Argument(name="sortKey", value="UPDATED_AT"),    
                        ], 
                        fields=[Field(name='edges', fields=[Field(name='node', fields=['id',Field(name="images", fields=[Field(name="edges", fields=[Field(name="node", fields=["id", Field(name="metafields", fields=[Field(name="edges", fields=[Field(name="node", fields=["id", "namespace", "value", "key", "description", "createdAt", "updatedAt", "type"])])])])])])])])]
                    )
                ]
            ),
        ),
    ],
    ids=[
        "Metafield query with 1 query_path(str)",
        "Metafield query with composite quey_path(List[2])",
    ]
)
def test_metafield_bulk_query(query_class, filter_field, start, end, expected):
    stream = query_class()
    assert stream.get(filter_field, start, end) == expected.render()