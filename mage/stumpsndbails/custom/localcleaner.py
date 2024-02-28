import shutil
if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
    

@custom
def transform_custom(*args, **kwargs):
    """
    args: The output from any upstream parent blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your custom logic here

    directory_path = "/home/src/rawData"
    player_info_path = "/home/src/player_info_by_matches"
    match_info_path = "/home/src/match_info_by_matches"

    shutil.rmtree(directory_path)
    shutil.rmtree(player_info_path)
    shutil.rmtree(match_info_path)

    print("successfully deleted")



    return None
