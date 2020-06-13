from unittest.mock import patch

from django.test import TransactionTestCase 

from allianceauth.eveonline.models import EveCharacter

from ..tasks import run_bulk_character_model_update
import copy

class TestBulkTasks(TransactionTestCase):

    def setUp(self):
        EveCharacter.objects.all().delete()

        EveCharacter.objects.create(
            character_id=1,
            character_name='character.name1',
            corporation_id=2345,
            corporation_name='character.corp.name',
            corporation_ticker='c.c.t',  # max 5 chars 
            alliance_id=None
        )

        EveCharacter.objects.create(
            character_id=2,
            character_name='character.name2',
            corporation_id=9876,
            corporation_name='character.corp.name',
            corporation_ticker='c.c.t',  # max 5 chars 
            alliance_id=3456,
            alliance_name='character.alliance.name',
        )
        EveCharacter.objects.create(
            character_id=3,
            character_name='character.name3',
            corporation_id=9876,
            corporation_name='character.corp.name',
            corporation_ticker='c.c.t',  # max 5 chars 
            alliance_id=3456,
            alliance_name='character.alliance.name',
        )

        EveCharacter.objects.create(
            character_id=4,
            character_name='character.name4',
            corporation_id=9876,
            corporation_name='character.corp.name',
            corporation_ticker='c.c.t',  # max 5 chars 
            alliance_id=3456,
            alliance_name='character.alliance.name',
        )


    @patch('globalmods.tasks.update_character')
    @patch('allianceauth.eveonline.providers.provider')
    @patch('globalmods.tasks.CHUNK_SIZE', 1)
    def test_run_bulk_character_update(
        self,
        mock_provider,
        mock_update_character
    ):
        affiliations = [{'character_id':1, 'corporation_id':5},
        {'character_id':2, 'corporation_id':9876, 'alliance_id':3456},
        {'character_id':3, 'corporation_id':9876, 'alliance_id':3456},
        {'character_id':4, 'corporation_id':9876, 'alliance_id':3456}]
        def get_affiliations():
            return copy.deepcopy(affiliations)

        mock_provider.client.Character.post_characters_affiliation.return_value.result.side_effect = get_affiliations

        names = [{'id':1, 'name':'character.name1'},
        {'id':2,  'name':'character.name2'},
        {'id':4,  'name':'character.name4_new'}]
        def get_names():
            return copy.deepcopy(names)
            
        mock_provider.client.Universe.post_universe_names.return_value.result.side_effect = get_names
        run_bulk_character_model_update()

        #self.assertEqual( len(mock_provider.mock_calls), 36)

        self.assertEqual(mock_update_character.apply_async.call_count, 2)
        self.assertIn(
            int(mock_update_character.apply_async.call_args_list[0][1]['args'][0]), [1, 4]
        )
        self.assertIn(
            int(mock_update_character.apply_async.call_args_list[1][1]['args'][0]), [1, 4]
        )

    @patch('globalmods.tasks.update_character')
    @patch('allianceauth.eveonline.providers.provider')
    @patch('globalmods.tasks.CHUNK_SIZE', 1)
    def test_run_bulk_character_update_affil_fail(
        self,
        mock_provider,
        mock_update_character
    ):
        def get_affiliations():
            raise Exception("Shits Broke Yo")

        mock_provider.client.Character.post_characters_affiliation.return_value.result.side_effect = get_affiliations

        names = [{'id':1, 'name':'character.name1'},
        {'id':2,  'name':'character.name2'},
        {'id':4,  'name':'character.name4_new'}]
        def get_names():
            return copy.deepcopy(names)
            
        mock_provider.client.Universe.post_universe_names.return_value.result.side_effect = get_names
        run_bulk_character_model_update()

        self.assertEqual( mock_provider.client.Character.post_characters_affiliation.call_count, 4)
        self.assertEqual( mock_provider.client.Universe.post_universe_names.call_count, 0)

    @patch('globalmods.tasks.update_character')
    @patch('allianceauth.eveonline.providers.provider')
    @patch('globalmods.tasks.CHUNK_SIZE', 1)
    def test_run_bulk_character_update_name_fail(
        self,
        mock_provider,
        mock_update_character
    ):

        affiliations = [{'character_id':1, 'corporation_id':5},
        {'character_id':2, 'corporation_id':9876, 'alliance_id':3456},
        {'character_id':3, 'corporation_id':9876, 'alliance_id':3456},
        {'character_id':4, 'corporation_id':9876, 'alliance_id':3456}]
        def get_affiliations():
            return copy.deepcopy(affiliations)

        mock_provider.client.Character.post_characters_affiliation.return_value.result.side_effect = get_affiliations

        def get_names():
            raise Exception("Shits Broke Yo!")
            
        mock_provider.client.Universe.post_universe_names.return_value.result.side_effect = get_names

        run_bulk_character_model_update()

        self.assertEqual( mock_provider.client.Character.post_characters_affiliation.call_count, 4)
        self.assertEqual( mock_provider.client.Universe.post_universe_names.call_count, 4)

        self.assertEqual(mock_update_character.apply_async.call_count, 1)
        self.assertEqual(
            int(mock_update_character.apply_async.call_args_list[0][1]['args'][0]), 1
        )
