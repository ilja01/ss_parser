import pytest
import pandas as pd
import datetime
from main import (
    replace_lv_characters_with_eng,
    split_district_and_street_address_into_2_strings,
    process_df_columns,
    prep_fresh_data_df
)

# Test data fixtures
@pytest.fixture
def sample_df():
    return pd.DataFrame({
        'price_raw': ['€123,456', '€78,900', '€250,000'],
        'room_cnt': ['2', '3', '4'],
        'm2': ['45.5', '75', '100.5'],
        'adress': ['CentrsBrivibas iela 1', 'VEFBrivibas gatve 214', 'ĀgenskalnsMārupes iela 10'],
        'link': ['link1', 'link2', 'link3'],
        'proj_type': ['Jaun.', 'Spec.', 'Jaun.']
    })


def test_replace_lv_characters():
    # Basic replacements
    assert replace_lv_characters_with_eng('āēīū') == 'aeiu'
    assert replace_lv_characters_with_eng('ĀĒĪŪ') == 'AEIU'
    
    # Mixed case
    assert replace_lv_characters_with_eng('Āgenskalns') == 'Agenskalns'
    
    # No Latvian characters
    assert replace_lv_characters_with_eng('Hello') == 'Hello'
    
    # Empty string
    assert replace_lv_characters_with_eng('') == ''
    
    # Special characters and spaces
    assert replace_lv_characters_with_eng('Rīga, Latvijā!') == 'Riga, Latvija!'


def test_split_district_and_street_address():
    # Test regular cases CentrsBrivibas iela 1
    district, street = split_district_and_street_address_into_2_strings('CentrsBrivibas iela 1')
    assert district == 'centrs'
    assert street == 'brivibas iela 1'
    
    # Test DzeguzkalnsTapesu 44	dzeguzkalns	tapesu 44
    district, street = split_district_and_street_address_into_2_strings('DzeguzkalnsTapesu 44')
    assert district == 'dzeguzkalns'
    assert street == 'tapesu 44'

    # Test VEF special case
    district, street = split_district_and_street_address_into_2_strings('VEFBrivibas gatve 214')
    assert district == 'vef'
    assert street == 'brivibas gatve 214'
    
    # Test Sampeteris-Pleskodale special case
    district, street = split_district_and_street_address_into_2_strings('Sampeteris-PleskodaleMargrietas 16')
    assert district == 'sampeteris-pleskodale'
    assert street == 'margrietas 16'
   

def test_process_df_columns(sample_df):
    processed_df = process_df_columns(sample_df)
    
    # Test numeric conversions
    assert processed_df['price'].dtype in ['int64', 'float64']
    assert processed_df['room_cnt'].dtype in ['int64', 'float64']
    assert processed_df['m2'].dtype in ['int64', 'float64']
    
    # Test specific values
    assert processed_df['price'].iloc[0] == 123456
    assert processed_df['room_cnt'].iloc[0] == 2
    assert processed_df['m2'].iloc[0] == 45.5
    
    # Test price_per_m2 calculation
    expected_price_per_m2 = round(123456 / 45.5, 1)
    assert processed_df['price_per_m2'].iloc[0] == expected_price_per_m2
    
    # Test address latinization
    assert 'ā' not in processed_df['adress_latin'].iloc[2]
    
    # Test district/street splitting
    assert processed_df['district'].iloc[0] == 'centrs'
    assert 'brivibas iela 1' in processed_df['street_address'].iloc[0]
    
    # Test link modification
    assert all(processed_df['link'].str.startswith('https://www.ss.lv/'))
    
    # Test timestamp addition
    assert isinstance(processed_df['extr_time'].iloc[0], datetime.datetime)


def test_prep_fresh_data():
    # Create test data
    now = datetime.datetime.now()
    old_time = now - datetime.timedelta(hours=24)
    
    df = pd.DataFrame({
        'extr_time': [now, now, old_time, now],
        'proj_type': ['Jaun.', 'Spec.', 'Jaun.', 'Jaun.'],
        'price_per_m2': [1000, 2000, 3000, 1500]
    })
    
    fresh_df = prep_fresh_data_df(df)
    
    # Test filtering
    assert len(fresh_df) == 2  # Only recent 'Jaun.' entries
    
    # Test sorting
    assert fresh_df['price_per_m2'].iloc[0] < fresh_df['price_per_m2'].iloc[1]
    
    # Test index
    assert 'n' in fresh_df.columns
    assert fresh_df['n'].iloc[0] == 1
    assert fresh_df['n'].iloc[1] == 2
