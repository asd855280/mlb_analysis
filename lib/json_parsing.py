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

# COMMAND ----------

def calculate_rbi(at_bat_event):
    rbi = 0
    if 'runners' in at_bat_event['events'][-1]:
        for each_runner in at_bat_event['events'][-1]['runners']:
            if each_runner['outcome_id'] == 'ERN':
                rbi += 1
        return rbi
    return rbi

# COMMAND ----------

def calculate_run(inn_half_events: list, hitter_id: str) -> int:
    run = 0
    for at_bat in inn_half_events:
        if 'at_bat' in at_bat:
            for each_pitch in at_bat['at_bat']['events']:
                if 'runners' in each_pitch:
                    for each_runner in each_pitch['runners']:
                        if each_runner['id'] == hitter_id and each_runner['outcome_id'] == 'ERN':
                            run = 1
                            return run
    return run

# COMMAND ----------

import json

def parse_pbp_info(pbp_info):
    if 'rescheduled' in pbp_info['game']:
        return 'postponed'
    pbp_list = []
    stadium_name = pbp_info['game']['venue']['name']
    stadium_id = pbp_info['game']['venue']['id']
    total_inngins = pbp_info['game']['innings']
    home_team_id = pbp_info['game']['home_team']
    away_team_id = pbp_info['game']['away_team']
    game_date = pbp_info['game']['scheduled']
    game_id = pbp_info['game']['id']

    # Iterate each inning
    for inn_n, each_inn in enumerate(total_inngins):

        if 'scoring' in each_inn:
            # Iterate each half
            for half in each_inn['halfs']:
                inn_half = half['half']
                # print(game_id, inn_n, inn_half)
                # Iterate each at bat
                for event in half['events']:

                    if 'at_bat' in event:
                        at_bat_id = event['at_bat']['id']
                        pitcher_id = event['at_bat']['pitcher']['id']
                        hitter_id = event['at_bat']['hitter']['id']
                        pitcher_first_name = event['at_bat']['pitcher']['first_name']
                        pitcher_last_name = event['at_bat']['pitcher']['last_name']
                        hitter_first_name = event['at_bat']['hitter']['first_name']
                        hitter_last_name = event['at_bat']['hitter']['last_name']

                        # Find outcome description of each at bat
                        if len(event['at_bat']['events']) == 0:
                            continue
                        if 'flags' in event['at_bat']['events'][-1]:
                            if event['at_bat']['events'][-1]['flags']['is_ab_over']:
                                at_bat_result_desc = event['at_bat']['description']
                                is_hitter_hit = event['at_bat']['events'][-1]['flags']['is_hit']
                                outcome_id = event['at_bat']['events'][-1]['outcome_id']

                            else:
                                for runner in event['at_bat']['events'][-1]['runners']:
                                    if runner['out']:
                                        at_bat_result_desc = runner['description'] + ' Runner out.'
                                        outcome_id = 'nA'

                        elif 'runners' in event['at_bat']['events'][-1]:
                            for runner in event['at_bat']['events'][-1]['runners']:
                                if runner['out']:
                                    at_bat_result_desc = runner['description'] + ' Runner out.'
                                    outcome_id = 'nA'
                        # Pitching change right at the end of at bat
                        elif 'lineup' in event['at_bat']['events'][-1]:
                            for each_pitch in event['at_bat']['events']:
                                if 'flags' in each_pitch:
                                    if each_pitch['flags']['is_ab_over']:
                                        at_bat_result_desc = event['at_bat']['description']
                                        outcome_id = each_pitch['outcome_id']
                                        break

                        # Calculate runs and RBIs
                        run = calculate_run(half['events'], hitter_id)
                        rbi = calculate_rbi(event['at_bat'])

                        # Get team id for each player
                        pitcher_team_id = home_team_id if inn_half == 'T' else away_team_id
                        hitter_team_id = home_team_id if inn_half == 'B' else away_team_id

                        pitcher_hand = 'nA'
                        hitter_hand = 'nA'
                        for pitch in event['at_bat']['events']:
                            if 'pitcher' in pitch:
                                if 'pitcher_hand' in pitch['pitcher']:
                                    pitcher_hand = pitch['pitcher']['pitcher_hand']
                                    hitter_hand = pitch['pitcher']['hitter_hand']
                                    break

                        at_bat_info = {}
                        at_bat_info.update({
                            "at_bat_id": at_bat_id,
                            "inning": inn_n,
                            "half": inn_half,
                            "pitcher_id": pitcher_id,
                            "pitcher_name": f"{pitcher_first_name} {pitcher_last_name}",
                            "pitcher_hand": pitcher_hand,
                            "pitcher_team_id": pitcher_team_id,
                            "hitter_id": hitter_id,
                            "hitter_name": f"{hitter_first_name} {hitter_last_name}",
                            "hitter_hand": hitter_hand,
                            "hitter_team_id": hitter_team_id,
                            "outcome_id": outcome_id,
                            "is_hit": is_hitter_hit,
                            "run": run,
                            "rbi": rbi,
                            "event_desc": at_bat_result_desc,
                            "stadium_name": stadium_name,
                            "stadium_id": stadium_id,
                            "game_date": game_date,
                            "game_id": game_id
                        })
                        json_str = json.dumps(at_bat_info)
                        pbp_list.append(json_str)
    return pbp_list
