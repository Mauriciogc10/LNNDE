Given tables

segment_map(segment_id, device_id)
device_map (device_id, ip_address, mapped_at)

“ABC”, 123
“ABC”, 456
“XYZ”, 123

Give me all devices mapped to Segment id “ABC” or “XYZ”

-- Select device_id from segment_map where  segment_id in (“ABC”, “XYZ)”

SELECT device_id
FROM segment_map
WHERE segment_id IN ('ABC', 'XYZ');


-- Give me all devices mapped to Segments “ABC” and “XYZ”
SELECT device_id
FROM segment_map
WHERE segment_id IN ('ABC', 'XYZ')
GROUP BY device_id
HAVING COUNT(DISTINCT segment_id) = 2;




-- Select device_id from segment_map where  segment_id  =“ABC” and “XYZ”

SELECT dm.device_id, dm.ip_address
FROM device_map dm
JOIN (
    SELECT device_id, MAX(mapped_at) AS max_mapped_at
    FROM device_map
    GROUP BY device_id
) max_map ON dm.device_id = max_map.device_id AND dm.mapped_at = max_map.max_mapped_at;











Give me a list of Device ID and the last IP address mapped to the Device
