with rn as(
    select
        session_id,
        event_name,
        ROW_NUMBER() OVER(
            PARTITION BY CONCAT(user_cid, '_', FORMAT_TIMESTAMP('%x %H%M', date_time), '_', ctx_platform_norm)
        order by
            date_time
        ) event_number
    from
        `bq_red_benchmark.event` 
    WHERE
        1 = 1
        and date_time >= '2018-01-01 00:00:00'
        and date_time < '2018-02-28 00:00:00'
),
fst as(
    select
        event_number thr,
        session_id
    from
        rn
    where
        event_name IN('Logged In')
),
allevents as(
    select
        rn.*,
        fst.thr
    from
        rn
    join fst on
        (
            rn.session_id = fst.session_id
        )
    where
        rn.event_number >= fst.thr
),
pre_pivot as(
    select
        session_id,
        event_name,
        thr,
        ROW_NUMBER() over(
            partition by session_id,
            thr
        order by
            event_number
        ) rn
    from
        allevents
),
pivot as(
    select
        session_id,
        thr,
        max( case when( rn = 9 ) then event_name end ) e9,
        max( case when( rn = 8 ) then event_name end ) e8,
        max( case when( rn = 7 ) then event_name end ) e7,
        max( case when( rn = 6 ) then event_name end ) e6,
        max( case when( rn = 5 ) then event_name end ) e5,
        max( case when( rn = 4 ) then event_name end ) e4,
        max( case when( rn = 3 ) then event_name end ) e3,
        max( case when( rn = 2 ) then event_name end ) e2,
        max( case when( rn = 1 ) then event_name end ) e1
    from
        pre_pivot
    group by
        1,
        2
),
dist as(
    select
        distinct session_id,
        thr,
        'Logged In' AS e1,
        e2,
        e3,
        e4,
        e5,
        e6,
        e7,
        e8,
        e9
    from
        pivot
),
e1_co AS (select e1,count(*) cnt from dist group by   e1 ),
e2_co AS (SELECT e1,e2,count(*) cnt from dist group by   e1,e2 ),
e3_co AS (SELECT e1,e2,e3,count(*) cnt from dist group by   e1,e2,e3 ),
e4_co AS (select e1,e2,e3,e4,count(*) cnt from dist group by   e1,e2,e3,e4 ),
e5_co AS (select e1,e2,e3,e4,e5,count(*) cnt from dist group by   e1,e2,e3,e4,e5 ),
e6_co AS (select e1,e2,e3,e4,e5,e6,count(*) cnt from dist group by   e1,e2,e3,e4,e5,e6 ),
e7_co AS (select e1,e2,e3,e4,e5,e6,e7,count(*) cnt from dist group by   e1,e2,e3,e4,e5,e6,e7 ),
e8_co AS (select e1,e2,e3,e4,e5,e6,e7,e8,count(*) cnt from dist group by   e1,e2,e3,e4,e5,e6,e7,e8 ),
e9_co AS (select e1,e2,e3,e4,e5,e6,e7,e8,e9,count(*) cnt from dist group by   e1,e2,e3,e4,e5,e6,e7,e8,e9),
partitionedData AS(select e1_co.e1 e1,e1_co.cnt e1_count,e2_co.e2 e2,e2_co.cnt e2_count,
e3_co.e3,e3_co.cnt e3_count,
e4_co.e4,e4_co.cnt e4_count,
e5_co.e5,e5_co.cnt e5_count,
e6_co.e6,e6_co.cnt e6_count,
e7_co.e7,e7_co.cnt e7_count,
e8_co.e8,e8_co.cnt e8_count,
e9_co.e9,e9_co.cnt e9_count
from e1_co join e2_co on(e1_co.e1=e2_co.e1)
join e3_co on(e3_co.e2=e2_co.e2 and e3_co.e1=e1_co.e1)
join e4_co on(e4_co.e3=e3_co.e3 and e4_co.e2=e2_co.e2 and e4_co.e1=e1_co.e1)
join e5_co on(e5_co.e4=e4_co.e4 and e5_co.e3=e3_co.e3 and e5_co.e2=e2_co.e2 and e5_co.e1=e1_co.e1)
join e6_co on(e6_co.e5=e5_co.e5 and e6_co.e4=e4_co.e4 and e6_co.e3=e3_co.e3 and e6_co.e2=e2_co.e2 and e6_co.e1=e1_co.e1)
join e7_co on(e7_co.e6=e6_co.e6 and e7_co.e5=e5_co.e5 and e7_co.e4=e4_co.e4 and e7_co.e3=e3_co.e3 and e7_co.e2=e2_co.e2 and e7_co.e1=e1_co.e1)
join e8_co on(e8_co.e6=e7_co.e6 and e8_co.e6=e6_co.e6 and e8_co.e5=e5_co.e5 and e8_co.e4=e4_co.e4 and e8_co.e3=e3_co.e3 and e8_co.e2=e2_co.e2 and e8_co.e1=e1_co.e1)
join e9_co on(e9_co.e6=e8_co.e6 and e9_co.e6=e7_co.e6 and e9_co.e6=e6_co.e6 and e9_co.e5=e5_co.e5 and e9_co.e4=e4_co.e4 and e9_co.e3=e3_co.e3 and e9_co.e2=e2_co.e2 and e8_co.e1=e1_co.e1))
SELECT
    e1,
    e2,
    e3,
    e4,
    e5,
    e6,
    e7,
    e8,
    e9,
    MAX( e1_count ) e1_count,
    MAX( e2_count ) e2_count,
    MAX( e3_count ) e3_count,
    MAX( e4_count ) e4_count,
    MAX( e5_count ) e5_count,
    MAX( e6_count ) e6_count,
    MAX( e7_count ) e7_count,
    MAX( e8_count ) e8_count,
    MAX( e9_count ) e9_count
FROM
    partitionedData
GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9
ORDER BY
    10 DESC,
    11 DESC,
    12 DESC,
    13 DESC,
    14 DESC,
    15 DESC,
    16 DESC,
    17 DESC,
    18 DESC;