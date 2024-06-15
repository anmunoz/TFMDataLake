curl -v -s -S -X POST http://127.0.0.1:5050 \
--header 'Content-Type: multipart/form-data; boundary=boundary; charset=utf-8' \
--header 'User-Agent: orion/0.10.0' \
--header 'NETREMOR-API-KEY: 29511c75-5b49-4a25-875a-9dc76c1656c4' \
--header "Fiware-Service: demo" \
--header "http.multipart.name: continuous-record-body" \
-d '--boundary
Content-Disposition: form-data; name="continuous-record-body"

{
    "subject_id": "5be5c7a95635b5577815b629bdc200958ffe9621c7f436d03067ff73366bc576",
    "record_id": "e07hh08c8b193hh2g3ghde29c75h9cb0g8b193hffe2907hh08c8b1",
    "name": "Pepe Fernandez",
    "birth_year": 1985,
    "diagnosis": "",
    "gender": "male",
    "dominant_hand": "right",
    "record_added_on": 1709821209693,
    "recorded_tasks": [
        {
            "task_id": "SD",
            "task_name": "Beber",
            "starts_at": 1709821384224,
            "ends_at": 1709821393444
        },
        {
            "task_id": "SD",
            "task_name": "Beber",
            "starts_at": 1709821414803,
            "ends_at": 1709821421230
        },
        {
            "task_id": "SD",
            "task_name": "Beber",
            "starts_at": 1709821442391,
            "ends_at": 1709821458510
        },
        {
            "task_id": "SD",
            "task_name": "Beber",
            "starts_at": 1709821465535,
            "ends_at": 1709821472342
        },
        {
            "task_id": "TB",
            "task_name": "Pasar pagina",
            "starts_at": 1709821494473,
            "ends_at": 1709821503905
        },
        {
            "task_id": "SN",
            "task_name": "Escribir",
            "starts_at": 1709821511151,
            "ends_at": 1709821526459
        },
        {
            "task_id": "SN",
            "task_name": "Escribir",
            "starts_at": 1709821531521,
            "ends_at": 1709821547173
        },
        {
            "task_id": "SD",
            "task_name": "Beber",
            "starts_at": 1709821550796,
            "ends_at": 1709821556633
        },
        {
            "task_id": "SD",
            "task_name": "Beber",
            "starts_at": 1709821559973,
            "ends_at": 1709821564513
        }
    ]
}

--boundary--'
