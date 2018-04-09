with rn as(
    select
        session_id,
        event_name,
        ROW_NUMBER() OVER(
            session_id
        order by
            date_time
        ) event_number
    from
         test.primary_events 
    WHERE
        1 = 1
        and date_time >= '2017-12-31 00:00:00'
        and date_time < '2018-03-02 00:00:00'
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
partitionedData AS(
    select
        e1,
        e2,
        e3,
        e4,
        e5,
        e6,
        e7,
        e8,
        e9,
        count(*) OVER(
            PARTITION by e1
        ) e1_count,
        count(*) OVER(
            PARTITION by e1,
            e2
        ) e2_count,
        count(*) OVER(
            PARTITION by e1,
            e2,
            e3
        ) e3_count,
        count(*) OVER(
            PARTITION by e1,
            e2,
            e3,
            e4
        ) e4_count,
        count(*) OVER(
            PARTITION by e1,
            e2,
            e3,
            e4,
            e5
        ) e5_count,
        count(*) OVER(
            PARTITION by e1,
            e2,
            e3,
            e4,
            e5,
            e6
        ) e6_count,
        count(*) OVER(
            PARTITION by e1,
            e2,
            e3,
            e4,
            e5,
            e6,
            e7
        ) e7_count,
        count(*) OVER(
            PARTITION by e1,
            e2,
            e3,
            e4,
            e5,
            e6,
            e7,
            e8
        ) e8_count,
        count(*) OVER(
            PARTITION by e1,
            e2,
            e3,
            e4,
            e5,
            e6,
            e7,
            e8,
            e9
        ) e9_count
    from
        dist
) SELECT
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