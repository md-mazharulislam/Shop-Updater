import json
import asyncio
import numpy
import csv
import aiohttp
import aiofiles
import requests
from requests.auth import HTTPBasicAuth


async def fixed_json_data() -> dict:
    async with aiohttp.ClientSession() as session:
        async with session.get(
            url="https://raw.githubusercontent.com/mdminhaz2003/Shop-Updater/master/data.json",
            allow_redirects=False
        ) as json_data:
            data = await json_data.text()
            return json.loads(data)


def create_schema(
        p_id: int,
        product_img_height: str,
        product_img_width: str,
        check_out_info: dict,
        date_added: str,
        date_available: str,
        description_info: dict,
        discount_allowed: int,
        ean: str,
        images_info: list,
        is_active: bool,
        is_fsk_18: bool,
        is_vpe_active: bool,
        keywords_info: dict,
        last_modified_date: str,
        manufacturer_id: int,
        meta_description_info: dict,
        meta_keywords_info: dict,
        meta_title_info: dict,
        name_info: dict,
        ordered_count: int,
        price: float,
        product_model: str,
        product_type_id: int,
        quantity: int,
        quantity_unit_id: int,
        details_template: str,
        graduated_quantity: int,
        group_permissions: list,
        min_order: int,
        on_site_map: bool,
        options_details_template: str,
        options_listing_template: str,
        price_status: int,
        properties_combis_quantity_check_mode: int,
        properties_dropdown_mode: str,
        show_added_date_time: bool,
        show_on_start_page: bool,
        show_price_offer: bool,
        show_properties_price: bool,
        show_quantity_info: bool,
        show_weight: bool,
        site_map_change_frequency: str,
        site_map_priortiy: str,
        start_page_sort_order: int,
        use_properties_combis_shipping_time: bool,
        use_properties_combis_weight: bool,
        shipping_costs: int,
        shipping_time_id: int,
        short_description_info: dict,
        sort_order: int,
        tax_class_id: int,
        url_info: dict,
        url_keywords_info: dict,
        viewed_count_info: dict,
        vpe_id: int,
        vpe_value: int,
        weight: int
) -> dict:
    schema = {
        "addonValues": {
            "productsImageHeight": product_img_height,
            "productsImageWidth": product_img_width
        },
        "checkoutInformation": check_out_info,
        "dateAdded": date_added,
        "dateAvailable": date_available,
        "description": description_info,
        "discountAllowed": discount_allowed,
        "ean": ean,
        "id": p_id,
        "images": images_info,
        "isActive": is_active,
        "isFsk18": is_fsk_18,
        "isVpeActive": is_vpe_active,
        "keywords": keywords_info,
        "lastModified": last_modified_date,
        "manufacturerId": manufacturer_id,
        "metaDescription": meta_description_info,
        "metaKeywords": meta_keywords_info,
        "metaTitle": meta_title_info,
        "name": name_info,
        "orderedCount": ordered_count,
        "price": price,
        "productModel": product_model,
        "productTypeId": product_type_id,
        "quantity": quantity,
        "quantityUnitId": quantity_unit_id,
        "settings": {
            "detailsTemplate": details_template,
            "graduatedQuantity": graduated_quantity,
            "groupPermissions": group_permissions,
            "minOrder": min_order,
            "onSitemap": on_site_map,
            "optionsDetailsTemplate": options_details_template,
            "optionsListingTemplate": options_listing_template,
            "priceStatus": price_status,
            "propertiesCombisQuantityCheckMode": properties_combis_quantity_check_mode,
            "propertiesDropdownMode": properties_dropdown_mode,
            "showAddedDateTime": show_added_date_time,
            "showOnStartpage": show_on_start_page,
            "showPriceOffer": show_price_offer,
            "showPropertiesPrice": show_properties_price,
            "showQuantityInfo": show_quantity_info,
            "showWeight": show_weight,
            "sitemapChangeFrequency": site_map_change_frequency,
            "sitemapPriority": site_map_priortiy,
            "startpageSortOrder": start_page_sort_order,
            "usePropertiesCombisShippingTime": use_properties_combis_shipping_time,
            "usePropertiesCombisWeight": use_properties_combis_weight
        },
        "shippingCosts": shipping_costs,
        "shippingTimeId": shipping_time_id,
        "shortDescription": short_description_info,
        "sortOrder": sort_order,
        "taxClassId": tax_class_id,
        "url": url_info,
        "urlKeywords": url_keywords_info,
        "viewedCount": viewed_count_info,
        "vpeID": vpe_id,
        "vpeValue": vpe_value,
        "weight": weight
    }
    return schema


all_schemas = numpy.array([])


async def create_all_schema() -> None:
    global all_schemas
    fixed_json_data_list = await fixed_json_data()
    update_data_set = numpy.array(list(csv.reader(open("./data.csv", "r"), delimiter=","))).astype("str")
    for csv_data in update_data_set[1:]:
        csv_data_product_id = int(csv_data[0])
        for json_data in fixed_json_data_list:
            json_data_product_id = int(json_data["id"])
            if csv_data_product_id == json_data_product_id:
                create_object = create_schema(
                    p_id=json_data["id"],
                    product_img_height=json_data["addonValues"]["productsImageHeight"],
                    product_img_width=json_data["addonValues"]["productsImageWidth"],
                    check_out_info=json_data["checkoutInformation"],
                    date_added=json_data["dateAdded"],
                    date_available=json_data["dateAvailable"],
                    description_info=json_data["description"],
                    discount_allowed=json_data["discountAllowed"],
                    ean=json_data["ean"],
                    images_info=json_data["images"],

                    is_active=bool(csv_data[5]),

                    is_fsk_18=json_data["isFsk18"],
                    is_vpe_active=json_data["isVpeActive"],
                    keywords_info=json_data["keywords"],
                    last_modified_date=json_data["lastModified"],
                    manufacturer_id=json_data["manufacturerId"],
                    meta_description_info=json_data["metaDescription"],
                    meta_keywords_info=json_data["metaKeywords"],
                    meta_title_info=json_data["metaTitle"],
                    name_info=json_data["name"],
                    ordered_count=json_data["orderedCount"],
                    price=json_data["price"],

                    product_model=csv_data[1],

                    product_type_id=json_data["productTypeId"],

                    quantity=int(csv_data[2]),

                    quantity_unit_id=json_data["quantityUnitId"],
                    details_template=json_data["settings"]["detailsTemplate"],
                    graduated_quantity=json_data["settings"]["graduatedQuantity"],
                    group_permissions=json_data["settings"]["groupPermissions"],
                    min_order=json_data["settings"]["minOrder"],
                    on_site_map=json_data["settings"]["onSitemap"],
                    options_details_template=json_data["settings"]["optionsDetailsTemplate"],
                    options_listing_template=json_data["settings"]["optionsListingTemplate"],
                    price_status=json_data["settings"]["priceStatus"],
                    properties_combis_quantity_check_mode=json_data["settings"]["propertiesCombisQuantityCheckMode"],
                    properties_dropdown_mode=json_data["settings"]["propertiesDropdownMode"],
                    show_added_date_time=json_data["settings"]["showAddedDateTime"],

                    show_on_start_page=bool(csv_data[6]),

                    show_price_offer=json_data["settings"]["showPriceOffer"],
                    show_properties_price=json_data["settings"]["showPropertiesPrice"],

                    show_quantity_info=bool(csv_data[4]),

                    show_weight=json_data["settings"]["showWeight"],
                    site_map_change_frequency=json_data["settings"]["sitemapChangeFrequency"],
                    site_map_priortiy=json_data["settings"]["sitemapPriority"],

                    start_page_sort_order=int(csv_data[8]) if csv_data[8] != "None" else json_data["settings"]["startpageSortOrder"],

                    use_properties_combis_shipping_time=json_data["settings"]["usePropertiesCombisShippingTime"],
                    use_properties_combis_weight=json_data["settings"]["usePropertiesCombisWeight"],
                    shipping_costs=json_data["shippingCosts"],

                    shipping_time_id=int(csv_data[3]),

                    short_description_info=json_data["shortDescription"],

                    sort_order=int(csv_data[7]) if csv_data[7] != "None" else json_data["sortOrder"],

                    tax_class_id=json_data["taxClassId"],
                    url_info=json_data["url"],
                    url_keywords_info=json_data["urlKeywords"],
                    viewed_count_info=json_data["viewedCount"],
                    vpe_id=json_data["vpeId"],
                    vpe_value=json_data["vpeValue"],
                    weight=json_data["weight"]
                )
                all_schemas = numpy.append(all_schemas, create_object)
                break
            else:
                continue
    async with aiofiles.open(
        file="update_data.json",
        mode="w"
    ) as update_data_file:
        write_file = await update_data_file.write(json.dumps(all_schemas.tolist()))
        print(write_file)
        print("update_data.json file written.")

asyncio.run(create_all_schema())

index_number = numpy.array([], dtype="i8")
for number in range(1, len(all_schemas)):
    if number % 500 == 0:
        index_number = numpy.append(index_number, number + 1)
    elif number % 500 != 0 and number == len(all_schemas) - 1:
        index_number = numpy.append(index_number, number)
    else:
        continue

first_index = 0
last_index = 0
username = "Wimmer-Construction@web.de"
pas_wod = "zQ3c%5YN8azi"
url = "https://www.wimmer-construction.de/api.php/v2/products"
headers = {'content-type': 'application/json'}

for number in index_number:
    last_index = number
    print({'first': first_index, 'last': last_index})
    request_data = json.dumps(all_schemas[first_index:last_index].tolist())
    response = requests.request(
        method="PUT",
        url=url,
        headers=headers,
        auth=HTTPBasicAuth(
            username=username,
            password=pas_wod
        ),
        data=request_data
    )
    print(response)
    first_index = last_index
