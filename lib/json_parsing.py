# Databricks notebook source
def parse_game_info(json_obj):    
    list_game_info = []
    for each_game in json_obj["games"]:
        game_info = dict()
        game_id = each_game['id']
        game_time = each_game['scheduled']
        stadium_name = each_game['venue']['name']
        home_team = each_game['home']['name']
        away_team = each_game['away']['name']
        game_info.update({
            "game_id": game_id
            , "game_time": game_time
            , "stadium_name": stadium_name
            , "home_team": home_team
            , "away_team": away_team
        })
        list_game_info.append(game_info)
    return list_game_info
