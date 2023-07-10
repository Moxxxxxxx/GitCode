SELECT * FROM 
ads.ads_team_ft_member_git_detail
where team_member in 
(
select team_member
FROM ads.ads_team_ft_rd_member_git_detail
group by team_member
)