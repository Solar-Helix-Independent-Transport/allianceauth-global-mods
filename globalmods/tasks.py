import logging

from bravado.exception import HTTPError
from celery import shared_task

from allianceauth.eveonline import providers

from allianceauth.eveonline.models import EveAllianceInfo
from allianceauth.eveonline.models import EveCharacter
from allianceauth.eveonline.models import EveCorporationInfo

from allianceauth.eveonline.tasks import update_alliance, update_character, update_corp

logger = logging.getLogger(__name__)

TASK_PRIORITY = 7
CHUNK_SIZE = 999

@shared_task
def run_model_update():
    # update existing corp models
    for corp in EveCorporationInfo.objects.all().values('corporation_id'):
        update_corp.apply_async(args=[corp['corporation_id']], priority=TASK_PRIORITY)

    # update existing alliance models
    for alliance in EveAllianceInfo.objects.all().values('alliance_id'):
        update_alliance.apply_async(args=[alliance['alliance_id']], priority=TASK_PRIORITY)

    #update existing character models if required
    run_bulk_character_model_update.apply_async(priority=TASK_PRIORITY-1)


@shared_task
def run_bulk_character_model_update():  # In Bulk!
    characters = EveCharacter.objects.all().values_list('character_id', flat=True)

    n = CHUNK_SIZE
    chunks = [list(characters[i * n:(i + 1) * n]) for i in range((characters.count() + n - 1) // n )]

    for character_chunk in chunks:
        try: # Get Affiliations for all the ID's
            affiliation = providers.provider.client.Character\
                .post_characters_affiliation(characters=character_chunk).result()
        except:
            logging.error("Bulk Lookup Failed")
            continue # bad chunk, skip

        names = [] 
        try: # Get Name for all the ID's
            names = providers.provider.client.Universe\
                .post_universe_names(ids=character_chunk).result() 
        except:
            logging.error("Bulk Name Lookup Failed")
            pass # can live with this failing, log the error and move on


        _affiliation_dict = {}  # build our results dict for easy lookup
        for character in affiliation: 
            char_id = character.pop('character_id')
            _affiliation_dict[str(char_id)] = character

        for character in names: # add names is we got them
            char_id = character.get('id')
            _affiliation_dict[str(char_id)]['name'] = character.get('name')

        # fetch current chars from DB
        chars = EveCharacter.objects.filter(character_id__in=character_chunk)\
            .values('character_id', 'corporation_id', 'alliance_id', 'character_name')  
        
        #check them all for changes
        for character in chars:
            char_id = str(character.get('character_id'))
            if char_id in _affiliation_dict: # sanity check
                # Corp Changes?
                corp_changed = character.get('corporation_id') != _affiliation_dict[char_id].get('corporation_id')
                
                # Alliance Changes?
                _alli = character.get('alliance_id')
                alliance_changed = _alli != _affiliation_dict[char_id].get('alliance_id', None)
                
                # Name Changes?
                name_changed = False
                fetched_name = _affiliation_dict[char_id].get('name', False)
                if fetched_name:
                    name_changed = character.get('character_name') != fetched_name

                # update if they have changed.
                if corp_changed or alliance_changed or name_changed:  
                    logger.debug("Updating {}: A:{} C:{} N:{}".format(
                            character.get('character_name'),
                            alliance_changed, 
                            corp_changed,
                            name_changed
                        ))
                    update_character.apply_async(args=[character.get('character_id')], priority=TASK_PRIORITY)
