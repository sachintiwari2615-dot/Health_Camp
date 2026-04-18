-- ============================================================
--  health_queries.sql
--  Reference SQL queries for the Health Camp Registration Portal
-- ============================================================


-- ── 1. PATIENTS ─────────────────────────────────────────────

-- All patients (newest first)
SELECT id, patient_id, name, age, gender, phone, email,
       city, blood_group, existing_condition, risk_level, registered_at
FROM patients
ORDER BY registered_at DESC
LIMIT 100;


-- Search patient by patient_id
SELECT *
FROM patients
WHERE patient_id = 'P0001';


-- Search patient by email
SELECT *
FROM patients
WHERE email ILIKE '%@gmail.com';


-- Patients by risk level
SELECT *
FROM patients
WHERE risk_level = 'High'   -- 'Low' | 'Medium' | 'High'
ORDER BY age DESC;


-- Patients by city
SELECT *
FROM patients
WHERE city = 'Delhi'
ORDER BY name;


-- Patients by existing condition
SELECT *
FROM patients
WHERE existing_condition = 'Diabetes'
ORDER BY age DESC;


-- ── 2. ANALYTICS ────────────────────────────────────────────

-- Risk level distribution
SELECT risk_level,
       COUNT(*) AS total,
       ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM patients
GROUP BY risk_level
ORDER BY
    CASE risk_level
        WHEN 'High'   THEN 1
        WHEN 'Medium' THEN 2
        WHEN 'Low'    THEN 3
        ELSE 4
    END;


-- Age group distribution
SELECT
    CASE
        WHEN age < 18  THEN '0-17'
        WHEN age < 35  THEN '18-34'
        WHEN age < 50  THEN '35-49'
        WHEN age < 65  THEN '50-64'
        ELSE '65+'
    END AS age_group,
    COUNT(*) AS total,
    ROUND(AVG(age), 1) AS avg_age
FROM patients
GROUP BY age_group
ORDER BY MIN(age);


-- City-wise patient count (top 10)
SELECT city, COUNT(*) AS total
FROM patients
GROUP BY city
ORDER BY total DESC
LIMIT 10;


-- Existing condition breakdown
SELECT
    COALESCE(existing_condition, 'None') AS condition,
    COUNT(*) AS total
FROM patients
GROUP BY existing_condition
ORDER BY total DESC;


-- Blood group distribution
SELECT blood_group, COUNT(*) AS total
FROM patients
GROUP BY blood_group
ORDER BY total DESC;


-- Gender-wise risk level cross-tab
SELECT gender,
       risk_level,
       COUNT(*) AS total
FROM patients
GROUP BY gender, risk_level
ORDER BY gender, risk_level;


-- High-risk patients grouped by condition (priority alerts)
SELECT existing_condition,
       COUNT(*) AS high_risk_count,
       ROUND(AVG(age), 1) AS avg_age
FROM patients
WHERE risk_level = 'High'
  AND existing_condition IS NOT NULL
GROUP BY existing_condition
ORDER BY high_risk_count DESC;


-- ── 3. CAMPS ────────────────────────────────────────────────

-- All camps with registration counts
SELECT c.id,
       c.camp_name,
       c.location,
       c.camp_date,
       c.capacity,
       COUNT(r.id) AS registered,
       c.capacity - COUNT(r.id) AS seats_left
FROM camps c
LEFT JOIN registrations r ON r.camp_id = c.id
GROUP BY c.id
ORDER BY c.camp_date;


-- Camps that are not yet full
SELECT c.id, c.camp_name, c.location, c.camp_date,
       c.capacity - COUNT(r.id) AS seats_left
FROM camps c
LEFT JOIN registrations r ON r.camp_id = c.id
GROUP BY c.id
HAVING c.capacity - COUNT(r.id) > 0
ORDER BY c.camp_date;


-- ── 4. REGISTRATIONS ────────────────────────────────────────

-- All registrations for a given camp
SELECT p.patient_id, p.name, p.age, p.gender,
       p.blood_group, p.existing_condition, p.risk_level,
       r.registered_at
FROM registrations r
JOIN patients p ON p.id = r.patient_id
WHERE r.camp_id = 1                  -- replace with desired camp id
ORDER BY r.registered_at DESC;


-- High-risk patients registered for any upcoming camp
SELECT p.name, p.age, p.existing_condition, p.risk_level,
       c.camp_name, c.camp_date
FROM registrations r
JOIN patients p ON p.id = r.patient_id
JOIN camps    c ON c.id = r.camp_id
WHERE p.risk_level = 'High'
  AND c.camp_date >= CURRENT_DATE
ORDER BY c.camp_date;


-- ── 5. MAINTENANCE ──────────────────────────────────────────

-- Count of all records
SELECT
    (SELECT COUNT(*) FROM patients)      AS total_patients,
    (SELECT COUNT(*) FROM camps)         AS total_camps,
    (SELECT COUNT(*) FROM registrations) AS total_registrations;


-- Duplicate email check (should be empty after UNIQUE constraint)
SELECT email, COUNT(*)
FROM patients
GROUP BY email
HAVING COUNT(*) > 1;