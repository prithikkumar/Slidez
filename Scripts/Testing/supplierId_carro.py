import requests
import json

# === GraphQL API Details ===
graphql_url = 'https://api.getcarro.com/graphql'
graphql_query = """query ($productIds: [ID!], $first: Int!) {
  products(productIds: $productIds, first: $first) {
    edges {
      node {
        id
        title
        descriptionHtml
        totalVariants
        supplierId
        supplierName
        supplierOnlineStoreUrl
        images {
          altText
          id
          mediaURL
          position
        }
        availableOptions {
          name
          position
          values
        }
        categories
        featuredImage {
          altText
          id
          mediaURL
        }
        featuredMedia {
          altText
          id
          mediaURL
        }
        brandDescription
        description
        updatedAt
      }
    }
  }
}"""

headers = {
    'Content-Type': 'application/json',
    'Authorization': 'Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6Im5JNVZmby1SVG5nMWNNUm9uZ0t2VSJ9.eyJtb2duZXQvYXNzb2NpYXRlZF9icmFuZF9pZCI6Im1reDBsMWprejIxdHNzNTRobnE1NHo0ZSIsImlzcyI6Imh0dHBzOi8vdnlybC51cy5hdXRoMC5jb20vIiwic3ViIjoiNHJVbFR5MkJVYnNXNTAxOVdRVW43Q3hyZDlLZUR1SDBAY2xpZW50cyIsImF1ZCI6InVybjpjYXJyby1wbGF0Zm9ybSIsImlhdCI6MTc0NDU3NjAwMywiZXhwIjoxNzQ3MTY4MDAzLCJzY29wZSI6InJlYWQ6ZGlyZWN0b3J5IHJlYWQ6cHJvZHVjdHMgd3JpdGU6cHJvZHVjdHMgcmVhZDpkb2NzIGNyZWF0ZTp1c2VycyByZWFkOm9yZGVycyB3cml0ZTpvcmRlcnMiLCJndHkiOiJjbGllbnQtY3JlZGVudGlhbHMiLCJhenAiOiI0clVsVHkyQlVic1c1MDE5V1FVbjdDeHJkOUtlRHVIMCIsInBlcm1pc3Npb25zIjpbInJlYWQ6ZGlyZWN0b3J5IiwicmVhZDpwcm9kdWN0cyIsIndyaXRlOnByb2R1Y3RzIiwicmVhZDpkb2NzIiwiY3JlYXRlOnVzZXJzIiwicmVhZDpvcmRlcnMiLCJ3cml0ZTpvcmRlcnMiXX0.bhCKskZBNKbpPP4uqIYIaWuk305OHg9TvUWLFXEIk6SvSYdvodiklwiVE_u27bo_B1McvzmEL-JC_dZvHdbQVYpfM_HEkyhNzzapBg1cpCYuOEBOjM7rnSyMBCclhgzrvoGjkWpfqZODBUSXHIy1GuknILVp-BOdAJwzs0gTOEbFqAK-QrcvVv7xDOjmp1LzS58fNyS7HBWF2wS09gsTo1TQpQ5bVEnkyIpe79g4XmvXvNCIuW0fnGjn4mVBCZ890gmeA8u-bMtK7Zx0p_B3Vzt9t3gXhpTFwKdoheaqL9XaBHRWjU_Ns5NDKPFTxRZAJupJhxKG3tivay5DxLHJVQ'
}

# === Replace with any valid product ID you want to test ===
sample_product_id = "zzxkr2jgwgf3jf9r33ho3081"

response = requests.post(
    graphql_url,
    headers=headers,
    json={
        "query": graphql_query,
        "variables": {
            "productIds": [sample_product_id],
            "first": 1
        }
    }
)

result = response.json()

# Pretty print the fetched product
edges = result.get("data", {}).get("products", {}).get("edges", [])
if edges:
    product = edges[0]['node']
    print(json.dumps(product, indent=2))
else:
    print("No product found.")
