SELECT
    p.patient_id,
    p.first_name AS patient_first_name,
    p.last_name AS patient_last_name,
    p.gender,
    p.city,
    p.insurance_type,

    a.appointment_id,
    a.appointment_date,
    a.status AS appointment_status,
    a.visit_type,
    a.fee,

    b.bill_id,
    b.amount_charged,
    b.insurance_paid,
    b.patient_paid,
    b.payment_status

FROM healthcare.patients p
JOIN healthcare.appointments a
    ON p.patient_id = a.patient_id
JOIN healthcare.billing b
    ON a.appointment_id = b.appointment_id
LIMIT 50;