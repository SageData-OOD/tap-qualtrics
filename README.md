# tap-qualtrics

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from [Survey Responses](https://api.qualtrics.com/api-reference/b3A6NjEwNDM-get-response-export-file)

- Incrementally pulls data based on the input state

---

## Quick Start

1. Install

    pip install git+https://github.com/SageData-OOD/tap-qualtrics

2. Create the config file

   Create a JSON file called `config.json`. Its contents should look like:

   ```json
    {
        "start_date": "2021-10-28",
        "data_center": "xxxx",
        "client_id": "xxxxxxxxxxx",
        "client_secret": "xxxxxxxxxxxxxxxxxx",
        "refresh_token": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
    }
    ```

   - The `starts_dte` specifies the date at which the tap will begin pulling data.

   - The `data_center` : Your qualtrics data_center id
     - [Where is my data_center?](https://api.qualtrics.com/api-reference/ZG9jOjg3NjYzMw-base-url-and-datacenter-i-ds)
     - Example - `url: https://<yourdatacenterid>.qualtrics.com`

   - The `refresh_token`: The qualtrics OAuth Refresh Token
   - The `client_id`: The qualtrics OAuth client id
   - The `client_secret`: The qualtrics OAuth client secret
   

4. Run the Tap in Discovery Mode

    tap-qualtrics -c config.json -d

   See the Singer docs on discovery mode
   [here](https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#discovery-mode).

5. Run the Tap in Sync Mode

    tap-qualtrics -c config.json --catalog catalog-file.json

---

Copyright &copy; 2021 SageData
