import json
import re

def parse_images_from_string(raw_value):
    """
    Cleans up and parses a malformed string of image data into a list of dictionaries.
    """

    # Clean up the string by replacing single quotes with double quotes
    cleaned_string = raw_value.replace("'", "\"")
    
    # Handle None values by replacing them with 'null' (valid in JSON)
    cleaned_string = re.sub(r": None", ": null", cleaned_string)
    
    # Ensure there's a comma between the dictionary objects if missing
    cleaned_string = re.sub(r"(?<=\})(?=\s*\{)", ",", cleaned_string)
    
    try:
        # Now, parse the cleaned string as JSON
        images_list = json.loads(cleaned_string)
        
        # Optionally, validate and structure the data
        valid_images = []
        for img in images_list:
            valid_image = {
                'altText': img.get('altText', ''),
                'id': img.get('id', ''),
                'mediaURL': img.get('mediaURL', ''),
                'position': int(img.get('position', 0))
            }
            valid_images.append(valid_image)
        return valid_images

    except Exception as e:
        print(f"❌ Failed parsing images\nRaw: {raw_value}\nError: {e}")
        return []


raw = "[{'altText': 'Womens Wallet - Zip Purse - Pastel Peach Purse - Bags/Zipper Wallets', 'id': 'k1qch1exzva0me2kw2dtxdsa', 'mediaURL': 'https://cdn.shopify.com/s/files/1/0211/7635/2832/products/uniquely-you-womens-wallet-zip-purse-pastel-peach-one-size-bags-wallets-570.jpg?v=1695338844', 'position': 1} {'altText': 'Womens Wallet - Zip Purse - Pastel Peach Purse - Bags/Zipper Wallets', 'id': 'n25jc2xy8d1eye2g3yfvhjqi', 'mediaURL': 'https://cdn.shopify.com/s/files/1/0211/7635/2832/products/uniquely-you-womens-wallet-zip-purse-pastel-peach-one-size-bags-wallets-629.jpg?v=1695338841', 'position': 0}]"
raw_none = "[{'altText': None, 'id': None, 'mediaURL': None, 'position': 0}]"

parsed_data = parse_images_from_string(raw)
print(parsed_data)

parsed_data_none = parse_images_from_string(raw_none)
print(parsed_data_none)