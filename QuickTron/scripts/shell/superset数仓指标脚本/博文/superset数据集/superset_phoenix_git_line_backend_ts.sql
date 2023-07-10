SELECT tt2.work_date,SUM(tt1.`SUM(current_lines_count)`) as back_end_codelines
FROM
(
SELECT work_date,sum(current_lines_count) AS `SUM(current_lines_count)`
FROM ads.ads_project_git_detail
WHERE ((second_level_directory = 'phoenix'
        and git_repository NOT LIKE '%software/phoenix/web%') OR (git_repository LIKE '%hardware/upper_computer/%upper_computer.git%'))
GROUP BY work_date
)tt1
LEFT JOIN
(
SELECT work_date,sum(current_lines_count) AS `SUM(current_lines_count)`
FROM ads.ads_project_git_detail
WHERE ((second_level_directory = 'phoenix'
        and git_repository NOT LIKE '%software/phoenix/web%') OR (git_repository LIKE '%hardware/upper_computer/%upper_computer.git%'))
GROUP BY work_date
)tt2
on tt1.work_date <= tt2.work_date
GROUP BY tt2.work_date
