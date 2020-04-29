"""This script produces and shoots snippets

Todo:
    Expand to use more than occurrence sets for the initial object
"""
import argparse

from LmServer.common.lmconstants import ENCODING, SnippetOperations
from LmServer.common.log import ConsoleLogger
from LmServer.common.snippet import SnippetShooter
from LmServer.db.borg_scribe import BorgScribe


# .............................................................................
def main():
    """Main method for script
    """
    parser = argparse.ArgumentParser(
        description='This script produces and shoots snippets')
    parser.add_argument(
        'occurrence_id', type=int,
        help='The id of the occurrence set to produce snippets for')
    parser.add_argument(
        'operation', type=str, help='The operation performed',
        choices=[
            SnippetOperations.ADDED_TO,
            SnippetOperations.DOWNLOADED,
            SnippetOperations.USED_IN,
            SnippetOperations.VIEWED])
    parser.add_argument(
        'post_filename', type=str,
        help='A file location to write out the snippet post')

    # Optional arguments
    parser.add_argument(
        '-o2ident', type=str, dest='obj2ident',
        help='Identifier for an optional target object (projection perhaps)')
    parser.add_argument(
        '-url', type=str, dest='url', help='A URL associated with this action')
    parser.add_argument(
        '-who', type=str, dest='who', help='Who initiated this action')
    parser.add_argument(
        '-agent', type=str, dest='agent',
        help='The agent used to initiate this action')
    parser.add_argument(
        '-why', type=str, dest='why', help='Why was this action initiated')

    args = parser.parse_args()

    shooter = SnippetShooter()

    scribe = BorgScribe(ConsoleLogger())
    scribe.open_connections()

    occ = scribe.get_occurrence_set(args.occurrence_id)
    occ.read_data(do_read_data=True)
    scribe.close_connections()

    # If we provide a second object identifer, only use the url argument
    # If we don't have a second object, fallback to the occ url if no url
    #    argument
    url = args.url

    if args.obj2ident is not None:
        mod_time = None  # WIll get current time
    else:
        mod_time = occ.mod_time
        if url is None:
            url = occ.metadata_url

    # Add snippets to shooter
    shooter.add_snippets(
        occ, args.operation, op_time=mod_time, obj2ident=args.obj2ident,
        url=url, who=args.who, agent=args.agent, why=args.why)

    if len(shooter.snippets) > 0:
        # Shoot snippets
        shooter.shoot_snippets(solr_post_filename=args.post_filename)
    else:
        with open(args.post_filename, 'w', encoding=ENCODING) as out_f:
            out_f.write('none')
