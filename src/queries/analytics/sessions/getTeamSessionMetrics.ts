import prisma from 'lib/prisma';
import clickhouse from 'lib/clickhouse';
import { runQuery, CLICKHOUSE, PRISMA } from 'lib/db';
import { EVENT_TYPE, SESSION_COLUMNS } from 'lib/constants';
import { QueryFilters } from 'lib/types';

export async function getTeamSessionMetrics(teamId: string, column: string, filters: QueryFilters) {
  return runQuery({
    [PRISMA]: () => relationalQuery(teamId, column, filters),
    [CLICKHOUSE]: () => clickhouseQuery(teamId, column, filters),
  });
}

async function relationalQuery(teamId: string, column: string, filters: QueryFilters) {
  const { parseTeamFilters, rawQuery } = prisma;
  const { filterQuery, joinSession, params } = await parseTeamFilters(
    teamId,
    {
      ...filters,
      eventType: EVENT_TYPE.pageView,
    },
    {
      joinSession: SESSION_COLUMNS.includes(column),
    },
  );
  const includeCountry = column === 'city' || column === 'subdivision1';

  return rawQuery(
    `
    select 
      ${column} as x,
      count(distinct website_event.session_id) as y
      ${includeCountry ? ', country' : ''}
    from website_event
    inner join team_website on website_event.website_id = team_website.website_id
    ${joinSession}
    where team_website.team_id = {{teamId::uuid}}
      and website_event.created_at between {{startDate}} and {{endDate}}
      and website_event.event_type = {{eventType}}
      ${filterQuery}
    group by ${column}
    ${includeCountry ? ', country' : ''}
    order by count(distinct website_event.session_id) desc
    limit 100
    `,
    {
      ...params,
      teamId, // Add teamId to the parameters
    },
  );
}

async function clickhouseQuery(
  teamId: string,
  column: string,
  filters: QueryFilters,
): Promise<{ x: string; y: number; country?: string }[]> {
  const { parseTeamFilters, rawQuery } = clickhouse;
  const { filterQuery, params } = await parseTeamFilters(teamId, {
    ...filters,
    eventType: EVENT_TYPE.pageView,
  });
  const includeCountry = column === 'city' || column === 'subdivision1';

  return rawQuery(
    `
    select
      ${column} as x,
      count(distinct session_id) as y
      ${includeCountry ? ', country' : ''}
    from website_event
    inner join team_website on website_event.website_id = team_website.website_id
    where team_website.team_id = {teamId:UUID}
      and website_event.created_at between {startDate:DateTime64} and {endDate:DateTime64}
      and event_type = {eventType:UInt32}
      ${filterQuery}
    group by ${column}
    ${includeCountry ? ', country' : ''}
    order by count(distinct session_id) desc
    limit 100
    `,
    {
      ...params,
      teamId, // Add teamId to the parameters
    },
  ).then(a => {
    return Object.values(a).map(a => {
      return { x: a.x, y: Number(a.y), ...(includeCountry && { country: a.country }) };
    });
  });
}
