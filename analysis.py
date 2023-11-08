"""
Analysis Example
Get users total transactions

This analysis must run by an Scheduled Action.
It gets a total amount of transactions by device, calculating by the total amount of data in the bucket
each time the analysis run. Group the result by a tag.

Environment Variables
In order to use this analysis, you must setup the Environment Variable table.

device_token: Token of a device where the total transactions will be stored. Get this in the Device's page.
account_token: Your account token. Check bellow how to get this.

Steps to generate an account_token:
1 - Enter the following link: https://admin.tago.io/account/
2 - Select your Profile.
3 - Enter Tokens tab.
4 - Generate a new Token with Expires Never.
5 - Press the Copy Button and place at the Environment Variables tab of this analysis.
"""
from tagoio_sdk import Analysis, Resources, Device
from tagoio_sdk.modules.Utils.envToJson import envToJson


def calculate_user_transactions(
    storage: Device, tag_value: str, device_list: list, account_token: str
) -> None:
    # Collect the data amount for each device.
    # Result of bucket_results is:
    # [0, 120, 500, 0, 1000]
    resources = Resources(params={"token": account_token})

    for device in device_list:
        total_transactions = resources.buckets.amount(device["bucket"])

        last_total_transactions = storage.getData(
            {"variable": "last_transactions", "qty": 1, "group": tag_value}
        )

        if not last_total_transactions:
            last_total_transactions = [{"value": 0}]

        last_total_transactions = last_total_transactions[0]

        result = total_transactions - last_total_transactions["value"]

        # Store the current total of transactions, the result for this analysis run and the key.
        # Now you can just plot these variables in a dynamic table.
        storage.sendData(
            data=[
                {"variable": "last_transactions", "value": total_transactions, "group": tag_value},
                {"variable": "transactions_result", "value": result, "group": tag_value},
                {"variable": "device_id", "value": device["id"], "group": tag_value},
            ]
        )

    print("Done!")


def my_analysis(context: any, scope: list = None) -> None:
    # Transform all Environment Variable to JSON.
    environment = envToJson(context.environment)

    if not environment.get("device_token") and not environment.get("account_token"):
        raise ValueError("You must setup an device_token and account_token in the Environment Variables.")

    storage = Device(params={"token": environment["device_token"]})

    # Get the device_list and group it by the tag value.
    resources = Resources()
    device_list = resources.devices.listDevice(
        {
            "page": 1,
            "fields": ["id", "name", "bucket", "tags"],
            "filter": {"tags": [{"key": "device_type", "value": "organization"}]},
            "amount": 10000,
        }
    )

    # Setup the tag we will be searching in the device list
    tag_value_search = "1234"

    group_device_list = []

    for device in device_list:
        # Check if the device has the tag we are searching for - org_id = 1234
        has_tag = list(filter(lambda tags: tags["key"] == "org_id" and tags["value"] == tag_value_search, device['tags']))

        if not has_tag:
            print(f"Device - {device['name']} does not have the tag org_id value {tag_value_search}")
            continue

        group_device_list.append(device)


    calculate_user_transactions(
        storage=storage,
        tag_value=tag_value_search,
        device_list=group_device_list,
        account_token=environment["account_token"],
    )


# The analysis token in only necessary to run the analysis outside TagoIO
Analysis.use(my_analysis, params={"token": "MY-ANALYSIS-TOKEN-HERE"})
