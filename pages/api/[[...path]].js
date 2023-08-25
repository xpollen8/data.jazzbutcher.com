const urlencode = require('urlencode');
const Db = require('mywrap');
const mysql = require('mysql2');

let db;

const queries = [
	// called by jazzbutcher.com
	{ noun: "gigs", query: "select * from gig where isdeleted IS NULL" },
	{ noun: "gigsongs", query: "select * from gigsong" },
	{ noun: "gigtexts", query: "select * from gigtext" },
	{ noun: "gigmedias", query: "select * from gigmedia" },
	{ noun: "performances", query: "select * from performance" },
	{ noun: "presses", query: "select * from press" },
	{ noun: "feedbacks", query: "select * from feedback where domain_id=11" },

	// others
	{ key: 'dtgig', noun: "press", query: "select * from press where ? order by album, dtpublished desc" },
	{ key: 'person', noun: "interviews_by_person", query: "select * from press where type like '%interview%' and {{key}}='{{value}}' order by dtpublished desc" },
	{ noun: "posters", query: "select datetime from gig where extra like '%poster%' and isdeleted IS NULL order by datetime desc" },
	{ noun: "gigtext_by_datetime", query: "select * from gigtext where datetime = '{{value}}'" },
	{ noun: "gigmedia_by_datetime", query: "select * from gigmedia where UNIX_TIMESTAMP(datetime) = UNIX_TIMESTAMP('{{value}}')" },
	{ noun: 'gig_by_datetime', key: 'datetime', query: "select * from gig where {{key}}='{{value}}' AND isdeleted IS NULL", joins: [
		//{ name: 'played', key: 'datetime', noun: 'gigsong' },
		//{ name: 'players', key: 'datetime', noun: 'performance' },
		//{ name: 'press', key: 'datetime', noun: 'press' },
		//{ name: 'media', key: 'datetime', noun: 'gigmedia' },
		{ name: 'text', key: 'datetime', noun: 'gigtext_by_datetime' },
		]
	},
	{ noun: 'gig', key: 'gig_id', query: "select * from gig where {{key}}={{value}} AND isdeleted IS NULL", joins: [
		//{ name: 'played', key: 'datetime', noun: 'gigsong' },
		//{ name: 'players', key: 'datetime', noun: 'performance' },
		//{ name: 'press', key: 'datetime', noun: 'press' },
		//{ name: 'media', key: 'datetime', noun: 'gigmedia' },
		{ name: 'text', key: 'datetime', noun: 'gigtext_by_datetime' },
		]
	},
	{ noun: "feedback", query: "select * from feedback where domain_id=11 and uri like '{{value}}%'" },
	{ key: 'datetime', noun: "gigsong", query: "select * from gigsong where ?" },
	{ key: 'datetime', noun: "performance", query: "select * from performance where ?" },
	{ noun: "gigs_and_year", query: "select *, year(datetime) as year from gig where isdeleted IS NULL order by datetime desc" },
	{ key: 'venue', noun: "gigs_by_venue", query: "select *, year(datetime) as year from gig where {{key}} like '%{{value}}%' and isdeleted IS NULL order by datetime desc" },
	{ noun: "gigs_and_year_by_id", query: "select *, year(datetime) as year from gig where find_in_set('{{id}}', extra) and isdeleted IS NULL order by datetime desc" },
	{ noun: "gigs_with_feedback", query: "select distinct(uri) from feedback where uri like 'gigs/%' order by uri desc" },
	{ noun: "gigs_with_video", query: "select gig_id, datetime, venue, address, city, state, postalcode, country, extra, blurb, title from gig where find_in_set('video', extra) and isdeleted IS NULL order by datetime desc" },
	{ key: 'lookup', noun: "album_personnel", query: "select * from performance where {{key}}='{{value}}'" },
	{ key: 'album', noun: "album_press", query: "select * from press where {{key}}='{{value}}' order by dtpublished" },
	{ noun: "gigs_with_audio", query: "select gs.*, g.extra, g.venue, g.city, g.country from gigsong gs, gig g where gs.mediaurl like '%audio/%' and gs.datetime = g.datetime and g.isdeleted IS NULL group by g.datetime order by gs.datetime desc, gs.type desc, gs.setnum, gs.ordinal" },
	{ key: 'song', noun: "live_performances_by_song", query: 'select count(*) as cnt from gigsong where {{key}}="{{value}}"' },
	{ key: 'song', noun: "live_performances_with_media_by_song", query: 'select count(*) as cnt from gigsong where {{key}}="{{value}}" and length(mediaurl) > 0' },
];

const doQuery = async (noun, key, type, value) => {
	try {
		const obj = queries.find(q => q.noun === noun);
		if (!obj) {
			return { noun, key, value, error: 'object not found' }
		}
		key = key || obj.key || 'id';
		//console.log("OBJ", { noun, key, value, obj });
		let sql = JSON.parse(JSON.stringify(obj.query));
		//console.log("Q", { sql, key, value });
		//if (obj.cache) {
			//const results = obj.cache;
			//return { noun, key, value, cached: true, 'numResults': results.length, 'rows': results };
		//}
		if (sql.indexOf(`{{key}}`) > 0) {
			sql = sql.replace('{{key}}', key);
		}
		if (sql.indexOf(`{{value}}`) > 0) {
			if (value) {
				if (typeof value === 'string') {
					value =  value.replace('+',' ');
				} else {
					value = JSON.parse(JSON.stringify(value));
				}
				sql = sql.replace('{{value}}', value);
			} else {
				//console.log("ID REQUIRED");
				return { noun, key, value, error: "ID REQUIRED" };
			}
		}
		//console.log("SQL", sql, [ { [key]: value } ]);
		let Q;
		if (type === 'like') {
			sql = sql.replace('?', `${key} like ?`);
			Q = mysql.format(sql, [ '%' + value + '%' ]);
		} else {
			Q = mysql.format(sql, [ { [key]: value } ]);
		}
		//console.log("Q", Q);
		//const X = await db.query(sql, [ { [key]: value } ])
		const X = await db.query(Q)
			.then(async results => {
				//console.log("RES", { key, results });
				const V = await Promise.all((obj.joins || []).map(async o => {
					const jname = o.name;
					const jkey = o.key;
					const jtype = o.type || 'is';
					const jnoun = o.noun;
					const joins = await Promise.all(results.map(async row => {
						if (row) {
							const jvalue = row[jkey];
							//console.log("ROW", { jkey, jvalue });
							if (!jvalue) {
								return { noun: jnoun, key: jkey, value, jvalue, error: 'no join value' };
							}
							//console.log("JKEY", { jnoun, jkey, jvalue });
							const res = await doQuery(jnoun, null, jtype, jvalue);
							//console.log("RES", { jname, jkey, jvalue, res });
							return { [jname]: res }
						}
					}));
					//console.log("JOINS", joins);
					//Object.keys(V).forEach(m => results[m] = V[m]);
					return joins;
				}));
				//console.log("V", V);
				//Object.keys(V).forEach(m => results[m] = V[m]);
				//console.log("V", results);
				if (!key) {	// only cache if no id
					//obj.cache = row;
				}
				//console.log("RET1", { noun, key, value, numResults: results.length, results });
				return { noun, key, value, numResults: results.length, results };
			})
			.catch(error => {
				return { noun, key, value, error };
			});
		//console.log("X", X);
		return X;
	} catch (e) {
		console.log("ERROR", e);
	}
}

const handler = async (req, res) => {
	try {
		if (!db) {
			db = await (new Db({
				host: process.env['JBC_MYSQL_HOST'],
				username: process.env['JBC_MYSQL_USERNAME'],
				user: process.env['JBC_MYSQL_USER'],
				database: process.env['JBC_MYSQL_DATABASE'],
				password: process.env['JBC_MYSQL_PASSWORD'],
				waitForConnection: process.env['JBC_MYSQL_WAITFORCONNECTION'],
				connectionLimit: process.env['JBC_MYSQL_CONNECTIONLIMIT'],
				queueLimit: process.env['JBC_MYSQL_QUEUELIMIT'],
			})).start();
		}
		//console.log("DB", db, process.env['JBC_MYSQL_HOST']);
		const { path = [] } = req.query;
		let [ noun, key, type, value ] = path;
		if (key === 'exact') {	// will override path-based queries
			value = path.slice(2).join('/'); // [ 'feedback', 'exact', 'lyrics', 'sea_madness.html' ] -> 'lyrics/sea_madness.html'
		} else if (!type) {					// gig/1974
			value = key;
			type = 'is';
			key = null;
		} else if (!value) {	//  gig/venue/Zabo
			value = type;
			type  = 'is';
		}
		//console.log("USE", { noun, key, type, value });
		const ret = await doQuery(noun, key, type, value);
		//console.log("FINAL", ret);
		res.json(ret);
	} catch (e) {
		console.log("ERROR", e);
		res.status(400).send(e);
	}
}

export default handler;
