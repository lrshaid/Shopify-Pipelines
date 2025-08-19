# Shopify Bulk Queries Collection
# Each query is designed for different data extraction needs

QUERIES = {
    "orders_with_line_items": {
        "name": "Orders with Line Items",
        "description": "Complete order data including line items, customer info, and addresses",
        "query": """
{
  orders {
    edges {
      node {
        id
        name
        createdAt
        processedAt
        currencyCode
        confirmed
        cancelledAt
        cancelReason
        closed
        closedAt
        displayFinancialStatus
        displayFulfillmentStatus
        email
        customer {
          id
          email
          firstName
          lastName
        }
        shippingAddress {
          firstName
          lastName
          address1
          address2
          city
          province
          country
          zip
        }
        billingAddress {
          firstName
          lastName
          address1
          address2
          city
          province
          country
          zip
        }
        currentSubtotalPriceSet { shopMoney { amount currencyCode } }
        currentTotalDiscountsSet { shopMoney { amount currencyCode } }
        currentTotalPriceSet { shopMoney { amount currencyCode } }
        currentTotalTaxSet { shopMoney { amount currencyCode } }
        currentShippingPriceSet { shopMoney { amount currencyCode } }
        lineItems {
          edges {
            node {
              id
              title
              sku
              quantity
              originalUnitPriceSet { shopMoney { amount currencyCode } }
            }
          }
        }
        tags
        updatedAt
      }
    }
  }
}
"""
    },
    
    "products_with_variants": {
        "name": "Products with Variants",
        "description": "Complete product catalog with variants, images, and inventory",
        "query": """
{
  products {
    edges {
      node {
        id
        title
        handle
        description
        vendor
        productType
        tags
        status
        createdAt
        updatedAt
        variants {
          edges {
            node {
              id
              title
              sku
              price
              compareAtPrice
              inventoryQuantity
              barcode
              taxable
            }
          }
        }
        images {
          edges {
            node {
              id
              url
              altText
              width
              height
            }
          }
        }
        collections {
          edges {
            node {
              id
              title
              handle
            }
          }
        }
      }
    }
  }
}
"""
    },
    
    "customers": {
        "name": "Customers",
        "description": "Customer data with orders and addresses",
        "query": """
{
  customers {
    edges {
      node {
        id
        email
        firstName
        lastName
        phone
        createdAt
        updatedAt
        numberOfOrders
        defaultAddress {
          firstName
          lastName
          address1
          address2
          city
          province
          country
          zip
          phone
        }
        addresses {
              id
              firstName
              lastName
              address1
              address2
              city
              province
              country
              zip
              phone
        }
      }
    }
  }
}
"""
    },
    
    "collections": {
        "name": "Collections",
        "description": "Product collections with their products",
        "query": """
{
  collections {
    edges {
      node {
        id
        title
        handle
        description
        updatedAt
        products {
          edges {
            node {
              id
              title
              handle
              vendor
              productType
              status
            }
          }
        }
      }
    }
  }
}
"""
    },
    
    "inventory_items": {
        "name": "Inventory Items",
        "description": "Inventory tracking data",
        "query": """
{
  inventoryItems {
    edges {
      node {
        id
        sku
        tracked
        createdAt
        updatedAt
        inventoryLevels {
          edges {
            node {
              id
              quantities (names: ["available", "on_hand"]){id name quantity updatedAt}
              location { id name }
            }
          }
        }
        variant {
          id
          title
          sku
          product { id title handle }
        }
      }
    }
  }
}
"""
    },
    
    "locations": {
        "name": "Locations",
        "description": "Store locations and inventory levels",
        "query": """
{
  locations {
    edges {
      node {
        id
        name
        address{
                address1
                address2
                city
                province
                country
                zip
        }
        createdAt
        updatedAt
        inventoryLevels {
          edges {
            node {
              id
              quantities (names: ["available", "on_hand"])  
                                  { 
                                  id 
                                  name 
                                  quantity 
                                  updatedAt
                                  } 
              item {
                id
                sku
                variant { id title product { id title } }
              }
            }
          }
        }
      }
    }
  }
}
"""
    }
}
