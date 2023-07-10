SELECT tt2.work_date,SUM(tt1.`SUM(current_lines_count)`) as front_end_codelines
FROM
(
SELECT work_date,sum(current_lines_count) AS `SUM(current_lines_count)`
FROM ads.ads_project_git_detail
WHERE work_date < STR_TO_DATE('2022-02-15', '%Y-%m-%d')
  AND ((git_repository LIKE '%software/phoenix/web%'))
GROUP BY work_date
)tt1
LEFT JOIN
(
SELECT work_date,sum(current_lines_count) AS `SUM(current_lines_count)`
FROM ads.ads_project_git_detail
WHERE work_date < STR_TO_DATE('2022-02-15', '%Y-%m-%d')
  AND ((git_repository LIKE '%software/phoenix/web%'))
GROUP BY work_date
)tt2
on tt1.work_date <= tt2.work_date
GROUP BY tt2.work_date