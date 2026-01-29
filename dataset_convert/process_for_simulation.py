import sqlite3
import pandas as pd
import numpy as np
import json
import glob
import os

DB_FILES = ['intsc_data_769.db', 'intsc_data_770.db', 'intsc_data_771.db', 'intsc_data_775.db', 'intsc_data_776.db',
            'intsc_data_777.db', 'intsc_data_778.db', 'intsc_data_779.db', 'intsc_data_780.db', 'intsc_data_781.db',
            'intsc_data_782.db', 'intsc_data_783.db', 'intsc_data_784.db', 'intsc_data_785.db']
OUTPUT_FILE = 'simulation_scenario.json'
FPS = 30  # based on 0.033367 timestep

def calculate_yaw(df):
    """
    (Optional: if angle doesn't exist)
    Calculates the yaw (heading) for every point in the trajectory.
    Uses the direction to the NEXT point.
    """
    # Shift X and Y to get the "next" position
    next_x = df['X'].shift(-1)
    next_y = df['Y'].shift(-1)
    
    # Calculate angle using arctan2(dy, dx)
    # Result is in radians, range [-pi, pi]
    yaw = np.arctan2(next_y - df['Y'], next_x - df['X'])
    
    # Fill the last nan value with the previous yaw (car didn't rotate instantly at the end)
    yaw.iloc[-1] = yaw.iloc[-2] if len(yaw) > 1 else 0.0
    
    return yaw

def process_single_db(db_path, file_id):
    conn = sqlite3.connect(db_path)
    
    tracks_df = pd.read_sql_query(
        """
        SELECT TRACK_ID, TYPE, TRACK_LENGTH, TRACK_WIDTH 
        FROM TRACKS 
        WHERE TYPE = 'Car'
        """, 
        conn
    )
    
    if tracks_df.empty:
        conn.close()
        return {}

    track_ids = tuple(tracks_df['TRACK_ID'].tolist())
    if len(track_ids) == 0:
        return {}
    
    # get trajectory for each agent
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'TRAJECTORIES_%'")
    traj_table_name = cursor.fetchone()[0]
    
    query = f"""
        SELECT TRACK_ID, TIME, X, Y
        FROM {traj_table_name}
        WHERE TRACK_ID IN {track_ids}
        ORDER BY TRACK_ID, TIME
    """
    traj_df = pd.read_sql_query(query, conn)
    conn.close()
    
    # 3. Process Data
    processed_agents = {}
    
    # Group by Agent to process their path
    for track_id, agent_traj in traj_df.groupby('TRACK_ID'):
        # Get metadata for this agent
        meta = tracks_df[tracks_df['TRACK_ID'] == track_id].iloc[0]
        
        # Calculate Yaw (Critical for Front-View Rendering)
        agent_traj = agent_traj.sort_values('TIME')
        agent_traj['YAW'] = calculate_yaw(agent_traj)
        
        # Downsample if needed (e.g., take every 3rd frame if data is 30Hz and we want 10Hz)
        # agent_traj = agent_traj.iloc[::3, :]
        
        # Structure for Simulation
        agent_data = {
            "agent_id": f"{file_id}_{track_id}",  # Unique ID across files
            "type": meta['TYPE'],
            "length": float(meta['TRACK_LENGTH']) if meta['TRACK_LENGTH'] else 4.5, # Default car length
            "width": float(meta['TRACK_WIDTH']) if meta['TRACK_WIDTH'] else 1.8,   # Default car width
            "waypoints": []
        }
        
        for _, row in agent_traj.iterrows():
            agent_data["waypoints"].append({
                "time": float(row['TIME']),
                "x": float(row['X']),
                "y": float(row['Y']),
                "z": 0.0,  # Waterloo is 2D, assume flat ground
                "yaw": float(row['YAW'])
            })
            
        processed_agents[agent_data["agent_id"]] = agent_data
        
    return processed_agents

def main():
    all_files = glob.glob(os.path.join(DB_FOLDER, '*.db'))
    master_scenario = {}
    
    print(f"Found {len(all_files)} database files.")
    
    for db_file in all_files:
        # Extract File ID (e.g., 'intsc_data_769.db' -> '769')
        file_id = os.path.basename(db_file).split('_')[-1].replace('.db', '')
        print(f"Processing File ID: {file_id}...")
        
        try:
            file_agents = process_single_db(db_file, file_id)
            master_scenario.update(file_agents)
        except Exception as e:
            print(f"Error processing {db_file}: {e}")

    # Save to JSON
    print(f"Saving {len(master_scenario)} agents to {OUTPUT_FILE}...")
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(master_scenario, f, indent=2)
    
    print("Done. File is ready for the AlpaSim loader.")

if __name__ == "__main__":
    main()