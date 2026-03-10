The current lead scoring system in 

lead_enrichment.py
 uses a 100-point scale to rank brands based on their social presence, activity, and advertising intent.

Here is the breakdown of how the 100 points are distributed:

1. Social Reach (Max 25 Points)
We use a log scale for followers so that smaller, high-intent brands aren't completely overshadowed by massive ones.

100k+ Followers: 25 pts
50k - 100k: 22 pts
10k - 50k: 18 pts
5k - 10k: 14 pts
1k - 5k: 10 pts
500 - 1k: 6 pts
1 - 500: 3 pts
2. Marketing Intent (20 Points)
This is a high-intent signal. If the brand is actively spending money on Facebook ads, they are much more likely to have a budget for growth tools.

Currently Running Ads: +20 pts
3. Recent Activity (Max 20 Points)
We measure "poking the market" by looking at how many posts they made in the last 30 days.

8+ Posts: 20 pts (Approx. 2+ posts/week)
5 - 7 Posts: 16 pts
3 - 4 Posts: 12 pts
1 - 2 Posts: 8 pts
Has older posts but none recently: 3 pts
4. Audience Engagement (Max 20 Points)
We calculate the Average Engagement per post (Likes + Shares + Reactions). This filters out brands with "ghost" followers who don't actually interact.

100+ Avg Engagement: 20 pts
50 - 100: 16 pts
20 - 50: 12 pts
5 - 20: 8 pts
1 - 5: 4 pts
5. Reachability (Max 15 Points)
We check if the brand has made it easy to be contacted outside of Facebook.

Email Available: +8 pts
Phone Number Available: +7 pts
Summary:
"The score is a 100-point 'Temperature' gauge. 25% is the size of their audience, 40% is how active and engaged they are right now, 20% is whether they are currently spending money on ads, and 15% is if we have direct contact info to reach out to them."



The Average Engagement is calculated using the most recent 10 posts from the brand's Facebook page.

Here is the step-by-step math:

Collect Totals: For those 10 posts, we sum up all:
Likes
Shares
Top Reactions (Love, Haha, Wow, etc.)
Calculate Averages: We divide each total by the number of posts (usually 10) to get the "Per Post" average for each metric:
avg_likes = Total Likes / 10
avg_shares = Total Shares / 10
avg_reactions = Total Reactions / 10
Final Engagement Score: We add these three averages together:
Avg Engagement = avg_likes + avg_shares + avg_reactions

Example: If a brand's last 10 posts had a total of 500 likes, 100 shares, and 200 reactions:

Avg Likes = 50
Avg Shares = 10
Avg Reactions = 20
Total Avg Engagement = 80
In our scoring logic, an 80 would fall into the 50 - 100 bracket, earning the brand 16 points toward their total score.