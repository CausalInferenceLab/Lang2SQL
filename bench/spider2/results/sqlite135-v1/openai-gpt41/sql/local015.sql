WITH motorcycle_parties AS (
  SELECT
    p.case_id,
    p.party_number,
    p.party_safety_equipment_1,
    p.party_safety_equipment_2,
    p.party_number_killed,
    -- 파티가 오토바이/스쿠터인지 확인
    (LOWER(COALESCE(p.statewide_vehicle_type, '')) LIKE '%motorcycle%' OR LOWER(COALESCE(p.statewide_vehicle_type, '')) LIKE '%scooter%') AS is_motorcycle,
    -- 헬멧 착용 여부: party_safety_equipment_1 또는 _2에 "motorcycle helmet used"가 명시적으로 포함되는 경우
    (
      LOWER(COALESCE(p.party_safety_equipment_1, '')) LIKE '%motorcycle helmet used%' OR
      LOWER(COALESCE(p.party_safety_equipment_2, '')) LIKE '%motorcycle helmet used%'
    ) AS helmet_used
  FROM parties p
),
filtered_motorcycle_parties AS (
  -- 오토바이 파티만 추출
  SELECT * FROM motorcycle_parties WHERE is_motorcycle = 1
),
helmet_stats AS (
  -- 케이스별로 "헬멧 착용 또는 미착용한 오토바이 파티가 1명 이상"인 사건을 집계
  SELECT
    case_id,
    MAX(CASE WHEN helmet_used THEN 1 ELSE 0 END) AS has_helmet,
    MAX(CASE WHEN helmet_used=0 THEN 1 ELSE 0 END) AS has_no_helmet
  FROM filtered_motorcycle_parties
  GROUP BY case_id
),
collision_helmet_group AS (
  -- 하나의 collision(case_id)에 대하여 'helmet', 'no_helmet' 그룹을 부여
  SELECT
    case_id,
    CASE
      WHEN has_helmet = 1 AND has_no_helmet = 0 THEN 'helmet'
      WHEN has_helmet = 0 AND has_no_helmet = 1 THEN 'no_helmet'
      ELSE NULL -- 혼재시 제외(명확한 그룹만 비교)
    END AS helmet_group
  FROM helmet_stats
  WHERE (has_helmet = 1 AND has_no_helmet = 0) OR (has_helmet = 0 AND has_no_helmet = 1)
),
collision_fatalities AS (
  -- 케이스별 전체 오토바이 사망자 수 집계
  SELECT
    fmp.case_id,
    SUM(COALESCE(fmp.party_number_killed,0)) AS motorcyclist_fatalities
  FROM filtered_motorcycle_parties fmp
  GROUP BY fmp.case_id
),
final_stats AS (
  -- 그룹별로 충돌건수 및 사망자수 집계
  SELECT
    chg.helmet_group,
    COUNT(DISTINCT chg.case_id) AS collision_count,
    SUM(COALESCE(cf.motorcyclist_fatalities,0)) AS total_fatalities
  FROM collision_helmet_group chg
  LEFT JOIN collision_fatalities cf
    ON chg.case_id = cf.case_id
  GROUP BY chg.helmet_group
)
SELECT
  helmet_group AS "helmet_usage",
  total_fatalities,
  collision_count,
  ROUND(
    CASE WHEN collision_count > 0 THEN
      100.0 * total_fatalities / collision_count
    ELSE NULL END, 2
  ) AS fatality_rate_percent
FROM final_stats
ORDER BY helmet_group;
