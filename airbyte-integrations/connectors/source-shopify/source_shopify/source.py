#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from typing import Any, List, Mapping, Tuple

import requests
from airbyte_cdk import AirbyteLogger
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from requests.exceptions import ConnectionError, InvalidURL, JSONDecodeError, RequestException, SSLError

from .auth import MissingAccessTokenError, ShopifyAuthenticator
from .streams.streams import (
    AbandonedCheckouts,
    Articles,
    BalanceTransactions,
    Blogs,
    Collections,
    Collects,
    Countries,
    CustomCollections,
    CustomerAddress,
    Customers,
    CustomerSavedSearch,
    DiscountCodes,
    Disputes,
    DraftOrders,
    FulfillmentOrders,
    Fulfillments,
    InventoryItems,
    InventoryLevels,
    Locations,
    MetafieldArticles,
    MetafieldBlogs,
    MetafieldCollections,
    MetafieldCustomers,
    MetafieldDraftOrders,
    MetafieldLocations,
    MetafieldOrders,
    MetafieldPages,
    MetafieldProductImages,
    MetafieldProducts,
    MetafieldProductVariants,
    MetafieldShops,
    MetafieldSmartCollections,
    OrderRefunds,
    OrderRisks,
    Orders,
    Pages,
    PriceRules,
    ProductImages,
    Products,
    ProductsGraphQl,
    ProductVariants,
    Shop,
    SmartCollections,
    TenderTransactions,
    Transactions,
)
from .utils import SCOPES_MAPPING, ShopifyAccessScopesError, ShopifyBadJsonError, ShopifyConnectionError, ShopifyWrongShopNameError


class ConnectionCheckTest:
    def __init__(self, config: Mapping[str, Any]):
        self.config = config
        # use `Shop` as a test stream for connection check
        self.test_stream = Shop(self.config)
        # setting `max_retries` to 0 for the stage of `check connection`,
        # because it keeps retrying for wrong shop names,
        # but it should stop immediately
        self.test_stream.max_retries = 0

    def describe_error(self, pattern: str, shop_name: str = None, details: Any = None, **kwargs) -> str:
        connection_check_errors_map: Mapping[str, Any] = {
            "connection_error": f"Connection could not be established using `Shopify Store`: {shop_name}. Make sure it's valid and try again.",
            "request_exception": f"Request was not successfull, check your `input configuation` and try again. Details: {details}",
            "index_error": f"Failed to access the Shopify store `{shop_name}`. Verify the entered Shopify store or API Key in `input configuration`.",
            "missing_token_error": "Authentication was unsuccessful. Please verify your authentication credentials or login is correct.",
            # add the other patterns and description, if needed...
        }
        return connection_check_errors_map.get(pattern)

    def test_connection(self) -> tuple[bool, str]:
        shop_name = self.config.get("shop")
        if not shop_name:
            return False, "The `Shopify Store` name is missing. Make sure it's entered and valid."

        try:
            response = list(self.test_stream.read_records(sync_mode=None))
            # check for the shop_id is present in the response
            shop_id = response[0].get("id")
            if shop_id is not None:
                return True, None
            else:
                return False, f"The `shop_id` is invalid: {shop_id}"
        except (SSLError, ConnectionError):
            return False, self.describe_error("connection_error", shop_name)
        except RequestException as req_error:
            return False, self.describe_error("request_exception", details=req_error)
        except IndexError:
            return False, self.describe_error("index_error", shop_name, response)
        except MissingAccessTokenError:
            return False, self.describe_error("missing_token_error")

    def get_shop_id(self) -> tuple[bool, str]:
        """
        We need to have the `shop_id` value available to have it passed elsewhere and fill-in the missing data.
        By the time this method is tiggered, we are sure we've passed the `Connection Checks` and have the `shop_id` value.
        """
        response = list(self.test_stream.read_records(sync_mode=None))
        shop_id = response[0].get("id")
        return shop_id if shop_id else None


class SourceShopify(AbstractSource):
    def check_connection(self, logger: AirbyteLogger, config: Mapping[str, Any]) -> Tuple[bool, any]:
        """
        Testing connection availability for the connector.
        """
        config["shop"] = self.get_shop_name(config)
        config["authenticator"] = ShopifyAuthenticator(config)
        return ConnectionCheckTest(config).test_connection()

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        Mapping a input config of the user input configuration as defined in the connector spec.
        Defining streams to run.
        """
        config["shop"] = self.get_shop_name(config)
        config["authenticator"] = ShopifyAuthenticator(config)
        # add `shop_id` int value
        config["shop_id"] = ConnectionCheckTest(config).get_shop_id()
        user_scopes = self.get_user_scopes(config)
        always_permitted_streams = ["MetafieldShops", "Shop", "Countries"]
        permitted_streams = [
            stream for stream, stream_scopes in SCOPES_MAPPING.items() if all(scope in user_scopes for scope in stream_scopes)
        ] + always_permitted_streams

        # before adding stream to stream_instances list, please add it to SCOPES_MAPPING
        stream_instances = [
            AbandonedCheckouts(config),
            Articles(config),
            BalanceTransactions(config),
            Blogs(config),
            Collections(config),
            Collects(config),
            CustomCollections(config),
            Customers(config),
            DiscountCodes(config),
            Disputes(config),
            DraftOrders(config),
            FulfillmentOrders(config),
            Fulfillments(config),
            InventoryItems(config),
            InventoryLevels(config),
            Locations(config),
            MetafieldArticles(config),
            MetafieldBlogs(config),
            MetafieldCollections(config),
            MetafieldCustomers(config),
            MetafieldDraftOrders(config),
            MetafieldLocations(config),
            MetafieldOrders(config),
            MetafieldPages(config),
            MetafieldProductImages(config),
            MetafieldProducts(config),
            MetafieldProductVariants(config),
            MetafieldShops(config),
            MetafieldSmartCollections(config),
            OrderRefunds(config),
            OrderRisks(config),
            Orders(config),
            Pages(config),
            PriceRules(config),
            ProductImages(config),
            Products(config),
            ProductsGraphQl(config),
            ProductVariants(config),
            Shop(config),
            SmartCollections(config),
            TenderTransactions(config),
            Transactions(config),
            CustomerSavedSearch(config),
            CustomerAddress(config),
            Countries(config),
        ]

        return [stream_instance for stream_instance in stream_instances if self.format_name(stream_instance.name) in permitted_streams]

    @staticmethod
    def get_user_scopes(config):
        session = requests.Session()
        url = f"https://{config['shop']}.myshopify.com/admin/oauth/access_scopes.json"
        headers = config["authenticator"].get_auth_header()
        try:
            response = session.get(url, headers=headers).json()
            access_scopes = [scope.get("handle") for scope in response.get("access_scopes")]
        except InvalidURL:
            raise ShopifyWrongShopNameError(url)
        except JSONDecodeError as json_error:
            raise ShopifyBadJsonError(json_error)
        except (SSLError, ConnectionError) as con_error:
            raise ShopifyConnectionError(con_error)

        if access_scopes:
            return access_scopes
        else:
            raise ShopifyAccessScopesError(response)

    @staticmethod
    def get_shop_name(config):
        split_pattern = ".myshopify.com"
        shop_name = config.get("shop")
        return shop_name.split(split_pattern)[0] if split_pattern in shop_name else shop_name

    @staticmethod
    def format_name(name):
        return "".join(x.capitalize() for x in name.split("_"))
