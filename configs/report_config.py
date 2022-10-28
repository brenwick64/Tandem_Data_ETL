# General Configurations
src_datetime_format = '%Y-%m-%dT%H:%M:%S'
tgt_datetime_format = '%Y-%m-%d %H:%M:%S'
hour_gap = 6

# Report Configurations
bolus_events = {
"report_name" : "bolus_events",
"columns" : ['eventDateTime', 'standard.insulinDelivered.completionDateTime', 
                    'description', 'bolusType', 'bolusRequestOptions', 'bg', 
                    'actualTotalBolusRequested', 'carbSize', 'foodBolusSize',
                    'standard.insulinDelivered.value', 'standard.foodDelivered', 'standard.correctionDelivered',
                    'standard.insulinRequested'
            ]
}
cgm_events = {
    "report_name" : "cgm_events",
    "columns" : ['eventDateTime', 'egv.estimatedGlucoseValue']
}
calibration_events = {
    "report_name" : "calibration_events",
    "columns" : ['eventDateTime', 'deviceType', 'eventTypeId']
}

