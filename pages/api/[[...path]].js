const urlencode = require('urlencode');
const Db = require('mywrap');
const mysql = require('mysql2');

let db;
let db_FEEDBACK;

const queries = [
	// called by jazzbutcher.com
	{ noun: "gigs", query: "select * from gig where isdeleted IS NULL" },
	{ noun: "gigsongs", query: "select * from gigsong order by datetime, type, setnum, ordinal" },
	{ noun: "gigtexts", query: "select * from gigtext" },
	{ noun: "gigmedias", query: "select * from gigmedia" },
	{ noun: "performances", query: "select * from performance" },
	{ noun: "gigs_by_musician", key: "p.performer", query: 'select * from performance p, gig g where {{key}} like "[[person:%{{value}}%" and p.datetime=g.datetime' },
	{ noun: "gigs_by_song", key: "s.song", query: 'select * from gigsong s, gig g where {{key}} like "%{{value}}%" and s.datetime=g.datetime' },
	{ noun: "presses", query: "select url, type, person, dtadded, dtpublished, dtgig, todo, album, thumb, images, audio, media, publication, location, title, headline, subhead, summary, source, credit, LENGTH(body) - LENGTH(REPLACE(body, ' ', '')) as bodycount from press" },
	{ noun: "presses_for_admin", query: "select * from press" },
	{ noun: "medias", query: "select * from media" },
	{ noun: "feedbacks", query: "select * from feedback where isdeleted <> 'T'" },
	{ noun: "feedback", query: 'select * from feedback where isdeleted <> "T" and uri = "{{value}}" order by dtcreated desc' },
	{ noun: "lyrics", query: "select * from lyrics order by title" },
	{ noun: "lyric_by_href", key: 'href', query: "select * from lyrics where {{key}} like '{{value}}%'" },
	{ noun: "unreleased_audio", query: "select * from media where type='audio' and length(lookup) = 0 and collection like '%session%' order by project, collection, ordinal" },
	{ noun: "audio", query: "select * from media where type='audio' order by project, collection, ordinal" },
	{ noun: "video", query: "select * from media where type='video' order by project, collection, ordinal" },
	{ noun: "release_video_by_project", key: 'project', query: "select * from media where ? and type='video' order by collection, ordinal" },
	{ noun: "live_video_by_project", key: 'project', query: "select * from media where ? and type='video' and datetime <> '0000-00-00 00:00:00' order by collection, ordinal" },
	{ noun: "release_audio_by_project", key: 'project', query: "select * from performance where ? and category='release' and media <> 'NULL' group by lookup, song order by lookup, ordinal" },
	{ noun: "releases_by_song", key: 'song', query: 'select distinct(lookup), media, version from performance where ? and category="release"' },
	{ noun: "songs_by_release", query: 'select *, song as title from performance where lookup = "{{value}}" and length(performer) = 0 order by type, ordinal' },
	{ noun: "credits_by_release", query: 'select * from performance where lookup="{{value}}" and length(performer) > 0' },
	{ noun: "songs_by_datetime", key: 'datetime', query: "select * from gigsong where {{key}} like '%{{value}}%'" },
	{ noun: "presses_by_release", key: 'album', query: "select url, type, person, dtadded, dtpublished, dtgig, todo, album, thumb, images, media, publication, location, title, headline, subhead, summary, source, credit from press where ? order by dtpublished" },
	{ noun: "press_by_href", key: 'url', query: 'select * from press where {{key}} like "/press/{{value}}%"' },

	// others
	{ key: 'dtgig', noun: "press", query: "select * from press where ? order by album, dtpublished desc" },
	{ key: 'person', noun: "interviews_by_person", query: 'select * from press where type like "%interview%" and {{key}}="{{value}}" order by dtpublished desc' },
	{ noun: "posters", query: "select datetime from gig where extra like '%poster%' and isdeleted IS NULL order by datetime desc" },
	{ noun: "prevgig", query: "select datetime from gig where isdeleted IS NULL and datetime < '{{value}}' order by datetime desc limit 1" },
	{ noun: "nextgig", query: "select datetime from gig where isdeleted IS NULL and datetime > '{{value}}' order by datetime limit 1" },
	{ noun: "gigtext_by_datetime", query: "select * from gigtext where datetime = '{{value}}'" },
	{ noun: "gigmedia_by_datetime", query: "select * from gigmedia where datetime = '{{value}}'" },
	{ noun: "recent_press", query: "select * from press where dtadded > now() - interval 1 year order by dtadded desc" },
	//{ noun: "recent_media", query: "select * from media where dtcreated > now() - interval 1 month order by dtcreated desc" },
	{ noun: "recent_media", query: "select g.*, gs.* from gigsong gs, gig g where gs.added > now() - interval 3 month and gs.datetime=g.datetime order by added desc" },
	{ noun: "recent_feedback", query: "select * from feedback where dtcreated > now() - interval 1 month and isdeleted <> 'T' order by dtcreated desc" },
	{ noun: "on_this_day", query: "select * from gig where month(datetime)=month(now()) and day(datetime)=day(now()) order by datetime" },
	{ noun: "media_by_song", key: 'name', query: "select * from media where ?" },
	{ noun: 'gig_by_datetime', key: 'datetime', query: "select *, CAST(datetime as CHAR) as datetime from gig where ? AND isdeleted IS NULL", joins: [
			{ name: 'played', key: 'datetime', noun: 'gigsong' },
			{ name: 'media', key: 'datetime', noun: 'gigmedia' },
			{ name: 'text', key: 'datetime', noun: 'gigtext' },
			{ name: 'players', key: 'datetime', noun: 'performance' },
			{ name: 'press', key: 'datetime', noun: 'press' },
			{ name: 'next', key: 'datetime', noun: 'nextgig' },
			{ name: 'prev', key: 'datetime', noun: 'prevgig' },
		]
	},
	{ noun: 'gig', key: 'gig_id', query: "select * from gig where {{key}}={{value}} AND isdeleted IS NULL", joins: [
			{ name: 'played', key: 'datetime', noun: 'gigsong' },
			{ name: 'media', key: 'datetime', noun: 'gigmedia' },
			{ name: 'text', key: 'datetime', noun: 'gigtext' },
			{ name: 'players', key: 'datetime', noun: 'performance' },
			{ name: 'press', key: 'datetime', noun: 'press' },
		]
	},
	{ noun: "performance_by_datetime", key: 'datetime', query: "select * from performance where ?" },
	{ noun: "gigsong_by_datetime", key: 'datetime', query: "select * from gigsong where ? order by type, setnum, ordinal" },
	{ key: 'dtgig', noun: "press", query: "select * from press where ?" },
	{ key: 'datetime', noun: "gigmedia", query: "select * from gigmedia where ?" },
	{ key: 'datetime', noun: "gigtext", query: "select * from gigtext where ?" },
	{ key: 'datetime', noun: "gigsong", query: "select * from gigsong where ? order by type, setnum, ordinal" },
	{ key: 'datetime', noun: "performance", query: "select * from performance where ?" },
	{ noun: "gigs_and_year", query: "select *, year(datetime) as year from gig where isdeleted IS NULL order by datetime desc" },
	{ key: 'venue', noun: "gigs_by_venue", query: 'select *, year(datetime) as year from gig where isdeleted IS NULL and {{key}} like "%{{value}}%" and isdeleted IS NULL order by datetime desc' },
	{ noun: "gigs_and_year_by_id", query: "select *, year(datetime) as year from gig where find_in_set('{{id}}', extra) and isdeleted IS NULL order by datetime desc" },
	{ noun: "gigs_with_feedback", query: "select distinct(uri) from feedback where isdeleted <> 'T' and uri like '%gigs/%' order by uri desc" },
	{ noun: "gigs_with_video", query: "select gig_id, datetime, venue, address, city, state, postalcode, country, extra, blurb, title from gig where find_in_set('video', extra) and isdeleted IS NULL order by datetime desc" },
	{ key: 'lookup', noun: "album_personnel", query: "select * from performance where ?" },
	{ noun: "gigs_with_audio", query: "select gs.*, g.extra, g.venue, g.city, g.country from gigsong gs, gig g where gs.mediaurl like '%audio/%' and gs.datetime = g.datetime and g.isdeleted IS NULL order by gs.datetime desc, gs.type desc, gs.setnum, gs.ordinal" },
	{ noun: "audio_by_project", key: 'g.extra', query: "select gs.*, g.extra, g.venue, g.city, g.country from gigsong gs, gig g where gs.mediaurl like '%audio/%' and gs.datetime = g.datetime and g.isdeleted IS NULL and {{key}} like '%{{value}}%' group by g.datetime order by gs.datetime desc, gs.type desc, gs.setnum, gs.ordinal" },
	{ key: 'song', noun: "live_performances_by_song", query: 'select * from gigsong gs, gig g where gs.datetime=g.datetime and {{key}}="{{value}}"' },
	{ key: 'song', key: 'song', noun: "performances_by_song", query: 'select * from performance where {{key}}="{{value}}" and media <> "NULL"' },
	{ key: 'song', noun: "live_performances_with_media_by_song", query: 'select * from gigsong where {{key}}="{{value}}" and length(mediaurl) > 0 order by type, setnum, ordinal' },
];

const doQuery = async (noun, key, type, value) => {
	try {
		//console.log("LOOKUP", noun, key, type, value);
		const obj = queries.find(q => q.noun === noun);
		if (!obj) {
			return { noun, key, value, error: 'object not found' }
		}
		key = key || obj.key || 'id';
		//console.log("OBJ", { noun, key, value, obj });
		let sql = JSON.parse(JSON.stringify(obj.query));
		/* NO CACHE FOR NOW
		if (obj.cache) {
			const results = obj.cache;
			return { noun, key, value, cached: true, 'numResults': results.length, 'rows': results };
		}
		*/
		if (sql.indexOf(`{{key}}`) > 0) {
			sql = sql.replace(/{{key}}/g, key);
		}
		if (sql.indexOf(`{{value}}`) > 0) {
			if (value) {
				if (typeof value === 'string') {
					value =  value.replace('+',' ');
				} else {
					value = JSON.parse(JSON.stringify(value));
				}
				sql = sql.replace(/{{value}}/g, value);
			} else {
				return { noun, key, value, error: "ID REQUIRED" };
			}
		}
		let Q;
		if (type === 'like') {
			sql = sql.replace('?', `${key} like ?`);
			Q = mysql.format(sql, [ '%' + value + '%' ]);
		} else {
			Q = mysql.format(sql, [ { [key]: value } ]);
		}
		//console.log("Q", Q);
		const thisResults = await db.query(Q)
			.then(async results => {
				//console.log("RES", { key, results });
				const joins = {};
				await Promise.all((obj.joins || []).map(async o => {
					const jname = o.name;
					const jkey = o.key;
					const jtype = o.type || 'is';
					const jnoun = o.noun;
					await Promise.all(results.map(async row => {
						if (row) {
							const jvalue = row[jkey];
							//console.log("ROW", { jkey, jvalue });
							if (!jvalue) {
								joins[jname] = { noun: jnoun, key: jkey, value, jvalue, error: 'no join value' };
								return;
							}
							const res = await doQuery(jnoun, null, jtype, jvalue);
							// only return '.results' for joined items.
							joins[jname] = res.results;
						}
					}));
				}));

				if (!key) {	// only cache if no id
					//obj.cache = row;
				}

				results.forEach((res, key) => {
					// add the named joins to the original record
					results[key] = { ...results[key], ...joins };
				})

				const ret = { noun, key, value, numResults: results.length, results };
				//console.log("SUB RETURN", ret);
				return ret;
			})
			.catch(error => {
				return { noun, key, value, error };
			});
		return thisResults;
	} catch (e) {
		console.log("ERROR", e);
	}
}

const handler = async (req, res) => {
	try {
		const { path = [] } = req.query;
		const method = req.method;
		let [ noun, key, type, value ] = path;
		if (key === 'exact') {	// will override path-based queries
			value = '/' + path.slice(2).join('/') || ''; // [ 'feedback', 'exact', 'lyrics', 'sea_madness.html' ] -> 'lyrics/sea_madness.html'
		} else if (!type) {					// gig/1974
			value = key;
			type = 'is';
			key = null;
		} else if (!value) {	//  gig/venue/Zabo
			value = type;
			type  = 'is';
		}
		if (method === 'DELETE' || method === 'POST' || noun === 'feedback_delete')  {
			if (!db_FEEDBACK) {
				db_FEEDBACK = await (new Db({
					host: process.env['JBC_MYSQL_HOST'],
					//username: process.env['JBC_MYSQL_USERNAME_FEEDBACK'],
					user: process.env['JBC_MYSQL_USER_FEEDBACK'],
					database: process.env['JBC_MYSQL_DATABASE'],
					password: process.env['JBC_MYSQL_PASSWORD_FEEDBACK'],
					//waitForConnection: process.env['JBC_MYSQL_WAITFORCONNECTION'],
					connectionLimit: process.env['JBC_MYSQL_CONNECTIONLIMIT'],
					queueLimit: process.env['JBC_MYSQL_QUEUELIMIT'],
					//timezone: 'utc',
				})).start();
			}
			if (noun === 'gigsong_edit')  {
				const deletes = [];
				const inserts = [];
				const edits = await Promise.all(req.body?.edits?.map(async (e) => {
					return await db_FEEDBACK.query('update gigsong set ? where gigsong_id = ?',
						[
							e,
							e.gigsong_id,
						]);
					})
				);

				res.json({ edits, deletes, inserts });
			} else if (noun === 'feedback_delete')  {
				const resX = await db_FEEDBACK.query('update feedback set isdeleted = ? where feedback_id = ?',
					[
						"T",
						value,
					]);
				//console.log("RES", resX);
				res.json(rets);
			} else {
				const { session, host, feedback_id, uri, subject, who, whence, comments } = req.body;
				//console.log("POST", { session, host, path, noun, key, type, value, body: req.body });
				switch (noun) {
					case 'feedback_by_page_new': {
						const resX = await db_FEEDBACK.query('insert IGNORE into `feedback` set `session` = ?, `host` = ?, `feedback_id` = NULL, `uri` = ?, `subject` = ?, `dtcreated` = NOW(), `who` = ?, `whence` = ?, `comments` = ?',
							[
								session,
								host,
								uri,
								subject,
								who || 'No Email Given',
								whence || 'No Location Given',
								comments
							]);
						return res.json(resX);
					}
					break;
					case 'feedback_by_page_reply': {
						if (!feedback_id) { return res.json({ error: 'missing: feedback_id' }); }
						const resX = await db_FEEDBACK.query('insert IGNORE into `feedback` set `session` = ?, `host` = ?, `feedback_id` = NULL, `parent_id` = ?, `uri` = ?, `subject` = ?, `dtcreated` = NOW(), `who` = ?, `whence` = ?, `comments` = ?',
							[
								session,
								host,
								feedback_id,
								uri,
								subject,
								who || 'No Email Given',
								whence || 'No Location Given',
								comments
							]);
						//console.log("XX", resX);
						return res.json(resX);
					}
					break;
					default:
						return res.json({ error: `unknown: ${noun}` });
				}
			}
		} else if (method === 'GET')  {
			if (!db) {
				db = await (new Db({
					host: process.env['JBC_MYSQL_HOST'],
					//username: process.env['JBC_MYSQL_USERNAME'],
					user: process.env['JBC_MYSQL_USER'],
					database: process.env['JBC_MYSQL_DATABASE'],
					password: process.env['JBC_MYSQL_PASSWORD'],
					//waitForConnection: process.env['JBC_MYSQL_WAITFORCONNECTION'],
					connectionLimit: process.env['JBC_MYSQL_CONNECTIONLIMIT'],
					queueLimit: process.env['JBC_MYSQL_QUEUELIMIT'],
					//timezone: 'utc',
				})).start();
			}
			//console.log("QUERY", { noun, key, type, value });
			const ret = await doQuery(noun, key, type, value);
			//console.log("OUTPUT", JSON.stringify(ret, null, 4));
			res.json(ret);
		}
	} catch (e) {
		console.log("ERROR", e);
		res.status(400).send(e);
	}
}

export default handler;
