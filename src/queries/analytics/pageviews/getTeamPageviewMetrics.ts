import prisma from 'lib/prisma';
import clickhouse from 'lib/clickhouse';
import { runQuery, CLICKHOUSE, PRISMA } from 'lib/db';
import { EVENT_TYPE, SESSION_COLUMNS } from 'lib/constants';
import { QueryFilters } from 'lib/types';

export async function getTeamPageviewMetrics(
  teamId: string,
  columns: string,
  filters: QueryFilters,
) {
  return runQuery({
    [PRISMA]: () => relationalQuery(teamId, columns, filters),
    [CLICKHOUSE]: () => clickhouseQuery(teamId, columns, filters),
  });
}

async function relationalQuery(teamId: string, column: string, filters: QueryFilters) {
  const { rawQuery, parseTeamFilters } = prisma;
  const { filterQuery, joinSession, params } = await parseTeamFilters(
    teamId,
    {
      ...filters,
      eventType: column === 'event_name' ? EVENT_TYPE.customEvent : EVENT_TYPE.pageView,
    },
    { joinSession: SESSION_COLUMNS.includes(column) },
  );

  let excludeDomain = '';
  if (column === 'referrer_domain') {
    excludeDomain =
      'and (website_event.referrer_domain != {{websiteDomain}} or website_event.referrer_domain is null)';
  }

  // Add a join to the team_website table to link teams to websites
  const teamJoin = `
    INNER JOIN team_website ON website_event.website_id = team_website.website_id
  `;

  return rawQuery(
    `
    select ${column} as x, count(*) as y
    from website_event
    ${joinSession}
    ${teamJoin} 
    where team_website.team_id = {{teamId::uuid}}  -- Use teamId to filter the results
      and website_event.created_at between {{startDate}} and {{endDate}}
      and event_type = {{eventType}}
      ${excludeDomain}
      ${filterQuery}
    group by ${column}
    order by count(*) desc
    limit 100
    `,
    {
      ...params,
      teamId,
    },
  );
}

async function clickhouseQuery(
  teamId: string, // Changed this from websiteId to teamId
  column: string,
  filters: QueryFilters,
): Promise<{ x: string; y: number }[]> {
  const { rawQuery, parseTeamFilters } = clickhouse;
  const { filterQuery, params } = await parseTeamFilters(
    teamId, // This will be used to filter by teamId
    {
      ...filters,
      eventType: column === 'event_name' ? EVENT_TYPE.customEvent : EVENT_TYPE.pageView,
    },
  );

  let excludeDomain = '';
  if (column === 'referrer_domain') {
    excludeDomain = 'and referrer_domain != {websiteDomain:String}';
  }

  // Add a join to the team_website table to link teams to websites
  const teamJoin = `
    INNER JOIN team_website ON website_event.website_id = team_website.website_id
  `;

  return rawQuery(
    `
    select ${column} as x, count(*) as y
    from website_event
    ${teamJoin} 
    where team_website.team_id = {teamId:UUID}  -- Use teamId to filter the results
      and website_event.created_at between {startDate:DateTime64} and {endDate:DateTime64}
      and event_type = {eventType:UInt32}
      ${excludeDomain}
      ${filterQuery}
    group by x
    order by y desc
    limit 100
    `,
    {
      ...params,
      teamId, // Add teamId to the parameters
    },
  ).then(a => {
    return Object.values(a).map(a => {
      return { x: a.x, y: Number(a.y) };
    });
  });
}
