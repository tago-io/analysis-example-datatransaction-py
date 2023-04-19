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
from tagoio_sdk import Analysis, Account, Device
from tagoio_sdk.modules.Utils.envToJson import envToJson


def calculate_user_transactions(
    account: Account, storage: Device, user_value: str, device_list: list
) -> None:
    # Collect the data amount for each device.
    # Result of bucket_results is:
    # [0, 120, 500, 0, 1000]
    for device in device_list:
        total_transactions = account.buckets.amount(device["bucket"])

        # Get the total transactions of the last analysis run.
        # Group is used to get only for this user.
        # You can change that to get a specific device for the user, instead of using a global storage device.
        # One way to do that is by just finding the device using a tag, see example:
        #
        # [user_device] = account.devices.list({'page': 1, 'fields': ['id', 'name', 'bucket', 'tags'], 'filter': {'tags': [{'key': 'user_device', 'value': user_value}]}, 'amount': 1})
        # device_token = Utils.getTokenByName(account, user_device['id'])
        # storage = Device({'token': device_token})
        last_total_transactions = storage.getData(
            {"variable": "last_transactions", "qty": 1, "group": user_value}
        )

        if not last_total_transactions:
            last_total_transactions = [{"value": 0}]

        last_total_transactions = last_total_transactions[0]

        result = total_transactions - last_total_transactions["value"]

        # Store the current total of transactions, the result for this analysis run and the key.
        # Now you can just plot these variables in a dynamic table.
        storage.sendData(
            data=[
                {
                    "variable": "last_transactions",
                    "value": total_transactions,
                    "group": user_value,
                },
                {
                    "variable": "transactions_result",
                    "value": result,
                    "group": user_value,
                },
                {"variable": "user", "value": user_value, "group": user_value},
            ]
        )

    print("Done!")


def my_analysis(context: any, scope: list = None) -> None:
    # Transform all Environment Variable to JSON.
    environment = envToJson(context.environment)

    if not environment.get("account_token"):
        raise ValueError(
            "You must setup an account_token in the Environment Variables."
        )

    elif not environment.get("device_token"):
        raise ValueError("You must setup an device_token in the Environment Variables.")

    # Instance the account class
    account = Account(params={"token": environment["account_token"]})
    storage = Device(params={"token": environment["device_token"]})

    # Setup the tag we will be searching in the device list
    tag_to_search = "user_email"

    # Get the device_list and group it by the tag value.
    device_list = account.devices.listDevice(
        {
            "page": 1,
            "fields": ["id", "name", "bucket", "tags"],
            "filter": {"tags": [{"key": tag_to_search}]},
            "amount": 10000,
        }
    )

    grouped_device_list = {}

    for device in device_list:
        tag_value = None

        for tag in device["tags"]:
            if tag["key"] == tag_to_search:
                tag_value = tag["value"]
                break

        if tag_value:
            if tag_value not in grouped_device_list:
                grouped_device_list[tag_value] = []
            grouped_device_list[tag_value].append(device)

    grouped_device_list = [
        {"value": key, "device_list": value}
        for key, value in grouped_device_list.items()
    ]

    # Call a new function for each group in assynchronous way.
    calculate_user_transactions(
        account=account,
        storage=storage,
        user_value=grouped_device_list[0]["value"],
        device_list=grouped_device_list[0]["device_list"],
    )


# The analysis token in only necessary to run the analysis outside TagoIO
Analysis(params={"token": "MY-ANALYSIS-TOKEN-HERE"}).init(my_analysis)
