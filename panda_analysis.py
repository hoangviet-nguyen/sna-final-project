import pandas as pd
from ydata_profiling import ProfileReport

# Load the CSV files
anime_df = pd.read_csv("data/Anime_Nodes.csv")

producer_df = pd.read_csv("data/Producer_nodes.csv")
producer_relation_df = pd.read_csv("data/Producer_Relations.csv")

studio_df = pd.read_csv("data/Studio_Nodes.csv")
studio_relation_df = pd.read_csv("data/Studio_Relations.csv")

voice_actor_df = pd.read_csv("data/VoiceActor_Nodes.csv")
voice_actor_relation_df = pd.read_csv("data/VoiceActor_Relations.csv")


# Generate individual reports
anime_profile = ProfileReport(anime_df, title="Anime Node Profiling Report", explorative=True)

producer_profile = ProfileReport(producer_df, title="Producer Node Profiling Report", explorative=True)
producer_relation_profile = ProfileReport(producer_relation_df, title="Relation Profiling Report", explorative=True)

studio_profile = ProfileReport(studio_df, title="Studio Node Profiling Report", explorative=True)
studio_relation_profile = ProfileReport(studio_relation_df, title="Relation Profiling Report", explorative=True)

voice_actor_profile = ProfileReport(voice_actor_df, title="Voice Actor Node Profiling Report", explorative=True)
voice_actor_relation_profile = ProfileReport(voice_actor_relation_df, title="Relation Profiling Report", explorative=True)


# Save the reports to HTML files
anime_profile.to_file("pandaReport/node/anime_node_report.html")

producer_profile.to_file("pandaReport/node/producer_node_report.html")
producer_relation_profile.to_file("pandaReport/relation/producer_relation_report.html")

studio_profile.to_file("pandaReport/node/studio_node_report.html")
studio_relation_profile.to_file("pandaReport/relation/studio_relation_report.html")

voice_actor_profile.to_file("pandaReport/node/voice_actor_node_report.html")
voice_actor_relation_profile.to_file("pandaReport/relation/voice_actor_relation_report.html")

# Combine Anime and Producer data
merged_df = producer_relation_df.merge(anime_df, left_on="source", right_on="id", suffixes=('_relation', '_anime'))
merged_df = merged_df.merge(producer_df, left_on="target", right_on="id", suffixes=('', '_producer'))

combined_profile = ProfileReport(merged_df, title="Combined Data Profiling Report", explorative=True)
combined_profile.to_file("pandaReport/combined/AniPro_combined_report.html")

# Combine Anime and Studio data
merged_df = studio_relation_df.merge(anime_df, left_on="source", right_on="id", suffixes=('_relation', '_anime'))
merged_df = merged_df.merge(studio_df, left_on="target", right_on="id", suffixes=('', '_studio'))

combined_profile = ProfileReport(merged_df, title="Combined Data Profiling Report", explorative=True)
combined_profile.to_file("pandaReport/combined/AniStu_combined_report.html")

# Combine Anime and Voice Actor data
merged_df = voice_actor_relation_df.merge(anime_df, left_on="source", right_on="id", suffixes=('_relation', '_anime'))
merged_df = merged_df.merge(voice_actor_df, left_on="target", right_on="id", suffixes=('', '_voice_actor'))

combined_profile = ProfileReport(merged_df, title="Combined Data Profiling Report", explorative=True)
combined_profile.to_file("pandaReport/combined/AniVa_combined_report.html")
