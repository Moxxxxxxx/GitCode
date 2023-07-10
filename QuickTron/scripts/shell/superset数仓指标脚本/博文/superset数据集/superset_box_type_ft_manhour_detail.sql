SELECT
	id,
	team_ft,
	team_group,
	team_sub_group,
	team_member,
	project_classify_name,
	work_create_date,
	work_summary,
	work_desc,
	work_type,
	work_status,
	work_check_date,
	work_hour,
	work_id,
	sprint_classify_name,
	CASE
	WHEN project_classify_name IN ('[FT] 货架到人产品线','[FT] 料箱到人产品线','[FT] 箱式搬运产品线') THEN '产品线'
	WHEN project_classify_name IN ('[FT]货架到人项目汇总','[FT]料箱搬运项目汇总','[FT]料箱到人项目汇总') THEN '项目'
	WHEN project_classify_name IN ('[职能]箱式FT-日常工作[测试]','[职能]箱式FT-日常工作[研发]','[职能]箱式FT-日常工作[产品]','【SVT】- QP料箱搬运') THEN '日常工作'
	ELSE 'UNKNOWN'END AS working_types
FROM
	ads.ads_team_ft_virtual_member_manhour_detail 
WHERE
	virtual_org_name IS NULL 
	AND team_ft = '箱式FT'
	AND project_classify_name IN (
		'[FT] 货架到人产品线',
		'[FT] 料箱到人产品线',
		'[FT] 箱式搬运产品线',
		'[FT]货架到人项目汇总',
		'[FT]料箱搬运项目汇总',
		'[FT]料箱到人项目汇总',
		'[职能]箱式FT-日常工作[测试]',
		'[职能]箱式FT-日常工作[研发]',
		'[职能]箱式FT-日常工作[产品]',
	'【SVT】- QP料箱搬运' 
	)