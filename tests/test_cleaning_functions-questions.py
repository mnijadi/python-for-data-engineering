from cleaning_functions import remove_duplicates


def test_remove_duplicates():
    # Define sample data and unique_key
    data = [
        {"id": 1, "name": "user_1"},
        {"id": 2, "name": "user_2"},
        {"id": 3, "name": "user_3"},
        {"id": 3, "name": "user_4"},
        {"id": 4, "name": "user_5"},
        {"id": 2, "name": "user_2"},
    ]
    unique_key = "id"

    # Call the function
    unique_data = remove_duplicates(data, unique_key)

    # Assert that duplicates were removed
    # assert len(unique_data) == some number based on your input
    # Assert the actual values
    expected_data = [
        {"id": 1, "name": "user_1"},
        {"id": 2, "name": "user_2"},
        {"id": 3, "name": "user_3"},
        {"id": 4, "name": "user_5"},
    ]
    assert unique_data == expected_data


# Run this with the command python -m pytest ./tests
