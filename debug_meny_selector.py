import requests
from bs4 import BeautifulSoup
import re


def analyze_meny_page(url):
    """Analyze the Meny page structure and print useful selectors."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36"
    }

    print(f"Fetching: {url}")
    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        print(f"Error: Status code {response.status_code}")
        return

    soup = BeautifulSoup(response.text, "lxml")

    # Try to find product list container using more generic approach
    print("\n1. Looking for common list containers:")
    list_elements = soup.find_all("ul")
    for i, ul in enumerate(list_elements):
        class_name = ul.get("class", ["no-class"])
        child_count = len(ul.find_all("li", recursive=False))
        print(
            f"List {i + 1}: class='{' '.join(class_name)}', direct children: {child_count}"
        )

        # Check if this looks like a product list
        product_like_items = 0
        for li in ul.find_all("li", recursive=False):
            # Count how many list items have product-like characteristics
            has_price = bool(li.find(string=lambda s: s and "kr" in s))
            has_image = bool(li.find("img"))
            has_title = bool(li.find("h3"))

            if has_price and (has_image or has_title):
                product_like_items += 1

        if product_like_items > 0:
            print(f"  ↳ Found {product_like_items} product-like items in this list")
            # Show the first li element structure
            first_li = ul.find("li")
            if first_li:
                print(f"  ↳ First item classes: {first_li.get('class')}")

    # Look for product divs directly
    print("\n2. Looking for product divs:")
    product_divs = soup.find_all("div", class_=lambda c: c and "product" in c.lower())
    print(f"Found {len(product_divs)} divs with 'product' in class")

    if product_divs:
        first_product = product_divs[0]
        print(f"First product div class: {first_product.get('class')}")
        print(f"Parent elements:")
        parent = first_product.parent
        for i in range(3):  # Show 3 levels up
            if parent:
                print(f"  Level {i + 1}: <{parent.name}> class='{parent.get('class')}'")
                parent = parent.parent

    # Look for all h3 elements (product titles)
    print("\n3. Product titles:")
    titles = soup.find_all("h3")
    print(f"Found {len(titles)} h3 elements")
    if titles:
        # Try to identify pattern of product titles
        common_classes = set()
        for title in titles:
            classes = title.get("class", [])
            for cls in classes:
                common_classes.add(cls)
        print(f"Common classes among h3 elements: {common_classes}")

    # Look for price elements
    print("\n4. Price elements:")
    price_elements = soup.find_all(
        string=lambda s: s and re.search(r"\d+[,\.]\d+\s*kr", s)
    )
    print(f"Found {len(price_elements)} elements with price text")
    if price_elements:
        # Show some examples
        for i, price in enumerate(price_elements[:3]):
            parent = price.parent
            print(
                f"Price {i + 1}: '{price}', parent: <{parent.name}> class='{parent.get('class')}'"
            )

    print("\n5. Schema.org products:")
    schema_products = soup.find_all(attrs={"itemtype": "http://schema.org/Product"})
    print(f"Found {len(schema_products)} elements with schema.org Product type")
    if schema_products:
        first_schema = schema_products[0]
        print(
            f"First schema.org product: <{first_schema.name}> class='{first_schema.get('class')}'"
        )
        if first_schema.parent:
            print(
                f"Parent: <{first_schema.parent.name}> class='{first_schema.parent.get('class')}'"
            )


if __name__ == "__main__":
    url = "https://meny.no/varer/meieri-egg/"
    analyze_meny_page(url)
