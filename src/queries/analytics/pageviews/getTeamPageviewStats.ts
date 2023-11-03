import clickhouse from 'lib/clickhouse';
import { CLICKHOUSE, PRISMA, runQuery } from 'lib/db';
import prisma from 'lib/prisma';
import { EVENT_TYPE } from 'lib/constants';
import { QueryFilters } from 'lib/types';

export async function getTeamPageviewStats(teamId: string, filters: QueryFilters) {
  return runQuery({
    [PRISMA]: () => relationalQuery(teamId, filters),
    [CLICKHOUSE]: () => clickhouseQuery(teamId, filters),
  });
}

async function relationalQuery(teamId: string, filters: QueryFilters) {
  const { timezone = 'utc', unit = 'day' } = filters;
  const { getDateQuery, parseTeamFilters, rawQuery } = prisma;
  const { filterQuery, joinSession, params } = await parseTeamFilters(teamId, {
    ...filters,
    eventType: EVENT_TYPE.pageView,
  });

  return rawQuery(
    `
    select
      ${getDateQuery('website_event.created_at', unit, timezone)} x,
      count(*) y
    from website_event
    inner join team_website on website_event.website_id = team_website.website_id
      ${joinSession}
    where team_website.team_id = {{teamId::uuid}}
      and website_event.created_at between {{startDate}} and {{endDate}}
      and event_type = {{eventType}}
      ${filterQuery}
    group by 1
    `,
    {
      ...params,
      teamId,
    },
  );
}

async function clickhouseQuery(
  teamId: string,
  filters: QueryFilters,
): Promise<{ x: string; y: number }[]> {
  const { timezone = 'UTC', unit = 'day' } = filters;
  const { parseTeamFilters, rawQuery, getDateStringQuery, getDateQuery } = clickhouse;
  const { filterQuery, params } = await parseTeamFilters(teamId, {
    ...filters,
    eventType: EVENT_TYPE.pageView,
  });

  return rawQuery(
    `
    select
      ${getDateStringQuery('g.t', unit)} as x, 
      g.y as y
    from (
      select 
        ${getDateQuery('created_at', unit, timezone)} as t,
        count(*) as y
      from website_event
      inner join team_website on website_event.website_id = team_website.website_id
      where team_website.team_id = {teamId:UUID}
        and created_at between {startDate:DateTime64} and {endDate:DateTime64}
        and event_type = {eventType:UInt32}
        ${filterQuery}
      group by t
    ) as g
    order by t
    `,
    {
      ...params,
      teamId,
    },
  ).then(a => {
    return Object.values(a).map(a => {
      return { x: a.x, y: Number(a.y) };
    });
  });
}
