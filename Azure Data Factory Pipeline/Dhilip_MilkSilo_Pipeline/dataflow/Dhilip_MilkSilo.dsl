parameters{
	PBatchEnd as string,
	PMaxTime as string
}
source(output(
		coliform_count_cfu_ml as string,
		density as string,
		fat_pct as string,
		freezing_point_c as string,
		mb_rt_minutes as string,
		ph as string,
		protein_pct as string,
		sample_id as string,
		sample_timestamp as timestamp,
		silo_id as string,
		snf_pct as string,
		temperature_c as string,
		titratable_acidity_pct as string,
		total_plate_count_cfu_ml as string
	),
	allowSchemaDrift: true,
	validateSchema: false,
	ignoreNoFilesFound: false,
	rowUrlColumn: 'milksourcepath',
	documentForm: 'arrayOfDocuments',
	wildcardPaths:['Dhilip_Projects/Smart Dairy Milk Silo/bronze/milksilo_????????_????.json']) ~> SRCMilkData
source(output(
		sample_id as string,
		sample_timestamp as timestamp,
		silo_id as string,
		temperature_c as double,
		ph as double,
		density as double,
		fat_pct as double,
		freezing_point_c as double,
		mb_rt_minutes as long,
		protein_pct as double,
		snf_pct as double,
		titratable_acidity_pct as double,
		coliform_count_cfu_ml as long,
		total_plate_count_cfu_ml as long
	),
	allowSchemaDrift: true,
	validateSchema: false,
	ignoreNoFilesFound: false,
	format: 'parquet') ~> SrcSvrMilkData
Ftr1MilkData derive(temperature_c_clean = iif(
    isNull(temperature_c) ||
    upper(trim(toString(temperature_c))) == 'SENSOR_ERROR' ||
    upper(trim(toString(temperature_c))) == 'NA' ||
    upper(trim(toString(temperature_c))) == 'N/A' ||
    upper(trim(toString(temperature_c))) == 'NULL',
    toDouble(null()),
    toDouble(temperature_c)
),
		ph_clean = iif(
    isNull(ph) ||
    upper(trim(ph)) == 'ERROR' ||
    upper(trim(ph)) == 'NA' ||
    upper(trim(ph)) == 'N/A' ||
    upper(trim(ph)) == 'NULL',
    toDouble(null()),
    toDouble(trim(ph))
),
		density_clean = iif(     isNull(density) ||     upper(trim(density)) == 'SENSOR_ERROR' ||     upper(trim(density)) == 'NA' ||     upper(trim(density)) == 'N/A' ||     upper(trim(density)) == 'NULL',     toDouble(null()),     toDouble(trim(density)) ),
		fat_pct_clean = iif(     isNull(fat_pct) ||     upper(trim(fat_pct)) == 'SENSOR_ERROR' ||     upper(trim(fat_pct)) == 'NA' ||     upper(trim(fat_pct)) == 'N/A' ||     upper(trim(fat_pct)) == 'NULL',     toDouble(null()),     toDouble(trim(fat_pct)) ),
		freezing_point_c_clean = iif(     isNull(freezing_point_c) ||     upper(trim(freezing_point_c)) == 'SENSOR_ERROR' ||     upper(trim(freezing_point_c)) == 'NA' ||     upper(trim(freezing_point_c)) == 'N/A' ||     upper(trim(freezing_point_c)) == 'NULL',     toDouble(null()),     toDouble(trim(freezing_point_c)) ),
		mb_rt_minutes_clean = iif(
    isNull(mb_rt_minutes) ||
    upper(trim(mb_rt_minutes)) == 'SENSOR_ERROR' ||
    upper(trim(mb_rt_minutes)) == 'NA' ||
    upper(trim(mb_rt_minutes)) == 'N/A' ||
    upper(trim(mb_rt_minutes)) == 'NULL',
    toLong(null()),
    toLong(trim(mb_rt_minutes))
),
		protein_pct_clean = iif(     isNull(protein_pct) ||     upper(trim(protein_pct)) == 'SENSOR_ERROR' ||     upper(trim(protein_pct)) == 'NA' ||     upper(trim(protein_pct)) == 'N/A' ||     upper(trim(protein_pct)) == 'NULL',     toDouble(null()),     toDouble(trim(protein_pct)) ),
		snf_pct_clean = iif(     isNull(snf_pct) ||     upper(trim(snf_pct)) == 'SENSOR_ERROR' ||     upper(trim(snf_pct)) == 'NA' ||     upper(trim(snf_pct)) == 'N/A' ||     upper(trim(snf_pct)) == 'NULL',     toDouble(null()),     toDouble(trim(snf_pct)) ),
		titratable_acidity_pct_clean = iif(     isNull(titratable_acidity_pct) ||     upper(trim(titratable_acidity_pct)) == 'SENSOR_ERROR' ||     upper(trim(titratable_acidity_pct)) == 'NA' ||     upper(trim(titratable_acidity_pct)) == 'N/A' ||     upper(trim(titratable_acidity_pct)) == 'NULL',     toDouble(null()),     toDouble(trim(titratable_acidity_pct)) ),
		coliform_count_cfu_ml_clean = iif(     isNull(coliform_count_cfu_ml) ||     upper(trim(coliform_count_cfu_ml)) == 'SENSOR_ERROR' ||     upper(trim(coliform_count_cfu_ml)) == 'NA' ||     upper(trim(coliform_count_cfu_ml)) == 'N/A' ||     upper(trim(coliform_count_cfu_ml)) == 'NULL',     toLong(null()),     toLong(trim(coliform_count_cfu_ml)) ),
		total_plate_count_cfu_ml_clean = iif(     isNull(total_plate_count_cfu_ml) ||     upper(trim(total_plate_count_cfu_ml)) == 'SENSOR_ERROR' ||     upper(trim(total_plate_count_cfu_ml)) == 'NA' ||     upper(trim(total_plate_count_cfu_ml)) == 'N/A' ||     upper(trim(total_plate_count_cfu_ml)) == 'NULL',     toLong(null()),     toLong(trim(total_plate_count_cfu_ml)) )) ~> DrvMilkSilo
DrvMilkSilo split(!isNull(sample_id)
&& trim(sample_id) != ''
&& !isNull(silo_id)
&& trim(silo_id) != '',
	disjoint: false) ~> CSMilkValidData@(ValidData, InValidData)
CSMilkValidData@ValidData window(over(sample_id),
	desc(sample_timestamp, true),
	row_num = rowNumber()) ~> WinMilkData
WinMilkData filter(row_num == 1) ~> Ftr2MilkData
Ftr2MilkData select(mapColumn(
		sample_id,
		sample_timestamp,
		silo_id,
		temperature_c = temperature_c_clean,
		ph = ph_clean,
		density = density_clean,
		fat_pct = fat_pct_clean,
		freezing_point_c = freezing_point_c_clean,
		mb_rt_minutes = mb_rt_minutes_clean,
		protein_pct = protein_pct_clean,
		snf_pct = snf_pct_clean,
		titratable_acidity_pct = titratable_acidity_pct_clean,
		coliform_count_cfu_ml = coliform_count_cfu_ml_clean,
		total_plate_count_cfu_ml = total_plate_count_cfu_ml_clean
	),
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true) ~> Select2MilkData
Select2MilkData split(!isNull(sample_id)

&&

!isNull(silo_id)

&&
(
    silo_id == 'SILO_01'
    || silo_id == 'SILO_02'
    || silo_id == 'SILO_03'
    || silo_id == 'SILO_04'
    || silo_id == 'SILO_05'
)

&&

!isNull(sample_timestamp)

&&

!isNull(temperature_c)
&& temperature_c >= 0

&&

!isNull(ph)
&& ph >= 0
&& ph <= 14

&&

!isNull(titratable_acidity_pct)
&& titratable_acidity_pct >= 0

&&

!isNull(fat_pct)
&& fat_pct >= 0

&&

!isNull(snf_pct)
&& snf_pct >= 0

&&

!isNull(density)
&& density > 0

&&

!isNull(protein_pct)
&& protein_pct >= 0

&&

!isNull(freezing_point_c)

&&

!isNull(mb_rt_minutes)
&& mb_rt_minutes >= 0

&&

!isNull(total_plate_count_cfu_ml)
&& total_plate_count_cfu_ml >= 0

&&

!isNull(coliform_count_cfu_ml)
&& coliform_count_cfu_ml >= 0,
	disjoint: false) ~> CDDataQualityMilkData@(ValidMilkData, InValidMilkData)
SrcSvrMilkData derive(temperature_check = iif(temperature_c >= 2 && temperature_c <= 6, 1, 0),
		ph_check = iif(ph >= 6.5 && ph <= 6.8, 1, 0),
		acidity_check = iif(titratable_acidity_pct >= 0.13 && titratable_acidity_pct <= 0.17, 1, 0),
		fat_check = iif(fat_pct >= 3 && fat_pct <= 8, 1, 0),
		snf_check = iif(snf_pct >= 8 && snf_pct <= 14, 1, 0),
		density_check = iif(density >= 1.026 && density <= 1.076, 1, 0),
		protein_check = iif(protein_pct >= 3 && protein_pct <= 4.5, 1, 0),
		freezing_point_check = iif(freezing_point_c >= -0.565 && freezing_point_c <= -0.525, 1, 0),
		mb_rt_check = iif(mb_rt_minutes >= 30 && mb_rt_minutes <= 480, 1, 0),
		total_plate_count_check = iif(total_plate_count_cfu_ml >= 0, 1, 0),
		coliform_check = iif(coliform_count_cfu_ml >= 0, 1, 0)) ~> derivedColumn1
derivedColumn1 filter(temperature_check == 1
&&
ph_check == 1
&&
acidity_check == 1
&&
fat_check == 1
&&
snf_check == 1
&&
density_check == 1
&&
protein_check == 1
&&
freezing_point_check == 1
&&
mb_rt_check == 1
&&
total_plate_count_check == 1
&&
coliform_check == 1) ~> FtrSvrMilkData
FtrSvrMilkData select(mapColumn(
		sample_id,
		sample_timestamp,
		silo_id,
		temperature_c,
		ph,
		density,
		fat_pct,
		freezing_point_c,
		mb_rt_minutes,
		protein_pct,
		snf_pct,
		titratable_acidity_pct,
		coliform_count_cfu_ml,
		total_plate_count_cfu_ml
	),
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true) ~> SctSvrMilkData
SRCMilkData derive(milk_source_timestamp = toTimestamp(
    concat(
	   substring(regexExtract(milksourcepath, `milksilo_(\d{8})_(\d{4})\.json`, 1), 1, 4), '-',
	   substring(regexExtract(milksourcepath, `milksilo_(\d{8})_(\d{4})\.json`, 1), 5, 2), '-',
	   substring(regexExtract(milksourcepath, `milksilo_(\d{8})_(\d{4})\.json`, 1), 7, 2), 'T',
	   substring(regexExtract(milksourcepath, `milksilo_(\d{8})_(\d{4})\.json`, 2), 1, 2), ':',
	   substring(regexExtract(milksourcepath, `milksilo_(\d{8})_(\d{4})\.json`, 2), 3, 2), ':00'
    ),
    'yyyy-MM-dd\'T\'HH:mm:ss'
)) ~> DrvMilkDataTS
DrvMilkDataTS filter(milk_source_timestamp > toTimestamp(
    substring($PMaxTime,1,19),
    'yyyy-MM-dd\'T\'HH:mm:ss'
)
&&
milk_source_timestamp <= toTimestamp(
    $PBatchEnd,
    'yyyy-MM-dd\'T\'HH:mm:ss'
)) ~> Ftr1MilkData
CDDataQualityMilkData@InValidMilkData sink(allowSchemaDrift: true,
	validateSchema: false,
	format: 'parquet',
	partitionFileNames:['error_milk_silo.parquet'],
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true,
	mapColumn(
		sample_id,
		sample_timestamp,
		silo_id,
		temperature_c,
		ph,
		density,
		fat_pct,
		freezing_point_c,
		mb_rt_minutes,
		protein_pct,
		snf_pct,
		titratable_acidity_pct,
		coliform_count_cfu_ml,
		total_plate_count_cfu_ml
	),
	partitionBy('hash', 1)) ~> SinkErrorMilkData
CDDataQualityMilkData@ValidMilkData sink(allowSchemaDrift: true,
	validateSchema: false,
	format: 'parquet',
	partitionFileNames:['silver_milk_silo.parquet'],
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true,
	mapColumn(
		sample_id,
		sample_timestamp,
		silo_id,
		temperature_c,
		ph,
		density,
		fat_pct,
		freezing_point_c,
		mb_rt_minutes,
		protein_pct,
		snf_pct,
		titratable_acidity_pct,
		coliform_count_cfu_ml,
		total_plate_count_cfu_ml
	),
	partitionBy('hash', 1)) ~> SinkSilverMilkData
SctSvrMilkData sink(allowSchemaDrift: true,
	validateSchema: false,
	format: 'parquet',
	partitionFileNames:['Gold_milk_silo.parquet'],
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true,
	mapColumn(
		sample_id,
		sample_timestamp,
		silo_id,
		temperature_c,
		ph,
		density,
		fat_pct,
		freezing_point_c,
		mb_rt_minutes,
		protein_pct,
		snf_pct,
		titratable_acidity_pct,
		coliform_count_cfu_ml,
		total_plate_count_cfu_ml
	),
	partitionBy('hash', 1)) ~> SinkGoldMilkFinalData