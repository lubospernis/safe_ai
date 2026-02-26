select
    permid,
    d0

from {{ source('raw', 'safe_microdata') }}

limit 10
