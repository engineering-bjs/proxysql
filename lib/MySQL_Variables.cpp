#include "MySQL_Variables.h"
#include "proxysql.h"

#include "MySQL_Session.h"
#include "MySQL_Data_Stream.h"
#include "SpookyV2.h"

#include <sstream>

extern const MARIADB_CHARSET_INFO * proxysql_find_charset_nr(unsigned int nr);
extern MARIADB_CHARSET_INFO * proxysql_find_charset_name(const char *name);

MySQL_Variables::MySQL_Variables(MySQL_Session* _session) {
	assert(_session);
	session = _session;

	for (auto i = 0; i < SQL_NAME_LAST; i++) {
		switch(i) {
		case SQL_SAFE_UPDATES:
		case SQL_SELECT_LIMIT:
		case SQL_SQL_MODE:
		case SQL_TIME_ZONE:
		case SQL_CHARACTER_SET_RESULTS:
		case SQL_CHARACTER_SET_CONNECTION:
		case SQL_CHARACTER_SET_CLIENT:
		case SQL_CHARACTER_SET_DATABASE:
		case SQL_ISOLATION_LEVEL:
		case SQL_TRANSACTION_READ:
		case SQL_SESSION_TRACK_GTIDS:
		case SQL_SQL_AUTO_IS_NULL:
		case SQL_COLLATION_CONNECTION:
		case SQL_NET_WRITE_TIMEOUT:
		case SQL_MAX_JOIN_SIZE:
			updaters[i] = new Generic_Updater();
			break;
		case SQL_CHARACTER_SET:
			updaters[i] = new Charset_Updater();
			break;
		case SQL_SET_NAMES:
			updaters[i] = new Names_Updater();
			break;
		default:
			updaters[i] = NULL;
		}
	}
}

MySQL_Variables::~MySQL_Variables() {
	for (auto u : updaters)
		delete u;
}

void print_backtrace(void);

void MySQL_Variables::client_set_value(int idx, const char* value) {
	if (!session || !session->client_myds || !session->client_myds->myconn) return;
	session->client_myds->myconn->variables[idx].hash = SpookyHash::Hash32(value,strlen(value),10);

	if (session->client_myds->myconn->variables[idx].value) {
		free(session->client_myds->myconn->variables[idx].value);
	}
	session->client_myds->myconn->variables[idx].value = strdup(value);
}

void MySQL_Variables::client_set_value(int idx, const std::string& value) {
	if (!session || !session->client_myds || !session->client_myds->myconn) return;
	session->client_myds->myconn->variables[idx].hash = SpookyHash::Hash32(value.c_str(),strlen(value.c_str()),10);

	if (session->client_myds->myconn->variables[idx].value) {
		free(session->client_myds->myconn->variables[idx].value);
	}
	session->client_myds->myconn->variables[idx].value = strdup(value.c_str());
}

const char* MySQL_Variables::client_get_value(int idx) {
	if (!session || !session->client_myds || !session->client_myds->myconn) return NULL;
	return session->client_myds->myconn->variables[idx].value;
}

uint32_t MySQL_Variables::client_get_hash(int idx) {
	if (!session || !session->client_myds || !session->client_myds->myconn) return 0;
	return session->client_myds->myconn->variables[idx].hash;
}

void MySQL_Variables::server_set_value(int idx, const char* value) {
	if (!session || !session->mybe || !session->mybe->server_myds || !session->mybe->server_myds->myconn) return;
	session->mybe->server_myds->myconn->variables[idx].hash = SpookyHash::Hash32(value,strlen(value),10);

	if (session->mybe->server_myds->myconn->variables[idx].value) {
		free(session->mybe->server_myds->myconn->variables[idx].value);
	}
	session->mybe->server_myds->myconn->variables[idx].value = strdup(value);
}

const char* MySQL_Variables::server_get_value(int idx) {
	if (!session || !session->mybe || !session->mybe->server_myds || !session->mybe->server_myds->myconn) return NULL;
	return session->mybe->server_myds->myconn->variables[idx].value;
}

uint32_t MySQL_Variables::server_get_hash(int idx) {
	if (!session || !session->mybe || !session->mybe->server_myds || !session->mybe->server_myds->myconn) return 0;
	return session->mybe->server_myds->myconn->variables[idx].hash;
}

bool MySQL_Variables::verify_generic_variable(uint32_t *be_int, char **be_var, char *def, uint32_t *fe_int, char *fe_var, enum session_status next_sess_status) {
	// be_int = backend int (hash)
	// be_var = backend value
	// def = default
	// fe_int = frontend int (has)
	// fe_var = frontend value
	if (*be_int == 0) {
		// it is the first time we use this backend. Set value to default
		if (*be_var) {
			free(*be_var);
			*be_var = NULL;
		}
		*be_var = strdup(def);
		uint32_t tmp_int = SpookyHash::Hash32(*be_var, strlen(*be_var), 10);
		*be_int = tmp_int;
	}
	if (*fe_int) {
		if (*fe_int != *be_int) {
			{
				*be_int = *fe_int;
				if (*be_var) {
					free(*be_var);
					*be_var = NULL;
				}
				if (fe_var) {
					*be_var = strdup(fe_var);
				}
			}
			switch(session->status) { // this switch can be replaced with a simple previous_status.push(status), but it is here for readibility
				case PROCESSING_QUERY:
					session->previous_status.push(PROCESSING_QUERY);
					break;
				case PROCESSING_STMT_PREPARE:
					session->previous_status.push(PROCESSING_STMT_PREPARE);
					break;
				case PROCESSING_STMT_EXECUTE:
					session->previous_status.push(PROCESSING_STMT_EXECUTE);
					break;
				default:
					proxy_error("Wrong status %d\n", session->status);
					assert(0);
					break;
			}
			session->set_status(next_sess_status);
			return true;
		}
	}
	return false;
}

bool MySQL_Variables::update_variable(session_status status, int &_rc) {
	int idx = SQL_NAME_LAST;
	for (int i=0; i<SQL_NAME_LAST; i++) {
		if (mysql_tracked_variables[i].status == status) {
			idx = i;
			break;
		}
	}
	assert(idx != SQL_NAME_LAST);
	return updaters[idx]->update_server_variable(session, idx, _rc);
}

bool MySQL_Variables::verify_variable(int idx) {
	auto ret = false;
	if (updaters[idx] && updaters[idx])
		ret = updaters[idx]->verify_variables(session, idx);
	return ret;
}

/* 
 * Updaters for different variables
 */

Updater::~Updater() {}


bool Generic_Updater::verify_variables(MySQL_Session* session, int idx) {
	auto ret = session->mysql_variables->verify_generic_variable(
		&session->mybe->server_myds->myconn->variables[idx].hash,
		&session->mybe->server_myds->myconn->variables[idx].value,
		mysql_thread___default_variables[idx],
		&session->client_myds->myconn->variables[idx].hash,
		session->client_myds->myconn->variables[idx].value,
		mysql_tracked_variables[idx].status
	);
	return ret;
}

bool Generic_Updater::update_server_variable(MySQL_Session* session, int idx, int &_rc) {
	bool no_quote = true;
	if (mysql_tracked_variables[idx].quote) no_quote = false;
	bool st = mysql_tracked_variables[idx].set_transaction;
	const char * set_var_name = mysql_tracked_variables[idx].set_variable_name;
	bool ret = false;

	/* character set variables store collation id in the char* string, but we set character_set_% command
	 * uses character set name or collation name. This branch convert collation id to character set name
	 * or collation name for further execution on backend
	 */
	if (idx==SQL_CHARACTER_SET_RESULTS) {
		const MARIADB_CHARSET_INFO *ci = NULL;
		ci = proxysql_find_charset_nr(atoi(session->mysql_variables->client_get_value(SQL_CHARACTER_SET_RESULTS)));

		/* CHARACTER_SET_RESULTS may have "NULL" and "binary" as parameter value. 
		 * -1 - NULL
		 * -2 - binary
		 *
		 *  TODO: current implementation is not nice. Think about nicer implementation
		 */
		if (!ci) {
			if (!strcmp(session->mysql_variables->client_get_value(SQL_CHARACTER_SET_RESULTS), "-1")) {
				ret = session->handler_again___status_SETTING_GENERIC_VARIABLE(&_rc, set_var_name, "NULL", no_quote, st);
			}
			else if (!strcmp(session->mysql_variables->client_get_value(SQL_CHARACTER_SET_RESULTS), "-2")) {
				ret = session->handler_again___status_SETTING_GENERIC_VARIABLE(&_rc, set_var_name, "binary", no_quote, st);
			}
		} else {
			ret = session->handler_again___status_SETTING_GENERIC_VARIABLE(&_rc, set_var_name, ci->csname, no_quote, st);
		}
	} else if (idx==SQL_COLLATION_CONNECTION) {
		const MARIADB_CHARSET_INFO *ci = NULL;
		ci = proxysql_find_charset_nr(atoi(session->mysql_variables->client_get_value(SQL_COLLATION_CONNECTION)));

		std::stringstream ss;
		ss << ci->nr;

		ret = session->handler_again___status_SETTING_GENERIC_VARIABLE(&_rc, set_var_name, ci->name, no_quote, st);
	} else if (idx==SQL_CHARACTER_SET_CONNECTION) {
		const MARIADB_CHARSET_INFO *ci = NULL;
		ci = proxysql_find_charset_nr(atoi(session->mysql_variables->client_get_value(SQL_CHARACTER_SET_CONNECTION)));

		unsigned int nr = ci->nr;
		std::stringstream ss;
		ss << nr;

		/* When CHARACTER_SET_CONNECTION is set the COLLATION_CONNECTION on backen is changed as well
		 * We track this change in the SQL_COLLATION_CONNECTION variable. We will need this value for
		 * verification in case client send 'set collation_connection=...' command
		 *
		 * case 1:
		 * set character_set_connection=utf8;
		 * select 1;
		 * set collation_connection=latin1;
		 * select 1;
		 *
		 */
		session->mysql_variables->client_set_value(SQL_COLLATION_CONNECTION, ss.str().c_str());

		ret = session->handler_again___status_SETTING_GENERIC_VARIABLE(&_rc, set_var_name, ci->csname, no_quote, st);
	} else if (idx==SQL_CHARACTER_SET_CLIENT || idx==SQL_CHARACTER_SET_DATABASE) {
		const MARIADB_CHARSET_INFO *ci = NULL;
		ci = proxysql_find_charset_nr(atoi(session->mysql_variables->client_get_value(idx)));

		std::stringstream ss;
		ss << ci->nr;
		ret = session->handler_again___status_SETTING_GENERIC_VARIABLE(&_rc, set_var_name, ci->csname, no_quote, st);
	} else {
		ret = session->handler_again___status_SETTING_GENERIC_VARIABLE(&_rc, set_var_name, session->mysql_variables->server_get_value(idx), no_quote, st);
	}
	return ret;
}

/* do verification of the SQL_CHARACTER_SET variable for 'set names' command
 */
bool Names_Updater::verify_variables(MySQL_Session* session, int idx) {

	/*
	 * we do verification only when
	 * 0 - 'set names' should be called from multiplexing
	 * 1 - 'set names' was called explicitly
	 * 3 - 'set names' was called from connect_start, handshake etc.
	 *
	 * TODO: 0 value can be changed to something more miningfull like MULTIPLEXING :)
	 */
	if (!strcmp(session->client_myds->myconn->variables[SQL_CHARACTER_ACTION].value, "1") ||
			!strcmp(session->client_myds->myconn->variables[SQL_CHARACTER_ACTION].value, "3") ||
			!strcmp(session->client_myds->myconn->variables[SQL_CHARACTER_ACTION].value, "0")) {

		auto ret = session->mysql_variables->verify_generic_variable(
				&session->mybe->server_myds->myconn->variables[SQL_CHARACTER_SET].hash,
				&session->mybe->server_myds->myconn->variables[SQL_CHARACTER_SET].value,
				mysql_thread___default_variables[SQL_CHARACTER_SET],
				&session->client_myds->myconn->variables[SQL_CHARACTER_SET].hash,
				session->client_myds->myconn->variables[SQL_CHARACTER_SET].value,
				mysql_tracked_variables[idx].status
				);

		/* in case of multiplexing all client variables should be set on server as is.
		 * for example, there is possibility that character_set_results differ from 'set names' character set.
		 * we should not change it here, so if client value differ from server value it will be set in the
		 * next iteration
		 */
		if (ret && !strcmp(session->client_myds->myconn->variables[SQL_CHARACTER_ACTION].value, "0")) {
			return ret;
		}

		/* client explicitly executed 'set names' */
		if (ret && !strcmp(session->client_myds->myconn->variables[SQL_CHARACTER_ACTION].value, "1")) {
			/* we should update clients' side of character variables, so the follwoing case will verify and update
			 * character variables.
			 *
			 * case 1:
			 * set name utf8;
			 * select 1;
			 * set character_set_results=latin1;
			 * select 1;
			 */
			session->mysql_variables->client_set_value(SQL_CHARACTER_SET_RESULTS, session->mysql_variables->server_get_value(SQL_CHARACTER_SET));
			session->mysql_variables->client_set_value(SQL_CHARACTER_SET_CLIENT, session->mysql_variables->server_get_value(SQL_CHARACTER_SET));
			session->mysql_variables->client_set_value(SQL_CHARACTER_SET_CONNECTION, session->mysql_variables->server_get_value(SQL_CHARACTER_SET));

			/* in mysql 8.0 default collation for utf8mb4 is utf8mb4_0900_ai_ci
			 * in mysql 5.7 default collation for utf8mb4 is utf8mb4_general_ci
			 *
			 * we agreed to have utf8mb4_general_ci for now
			 *
			 * There is possibility that proxysql will be connected to two mysql servers: 8.0 and 5.7. In this case
			 * we should use same collation to make sure that queries to different mysql servers return same resultset
			 */
			if ( !strcmp("45", session->mysql_variables->server_get_value(SQL_CHARACTER_SET)) && session->mybe->server_myds->myconn->mysql->server_version[0] == '8') {
				session->mysql_variables->client_set_value(SQL_COLLATION_CONNECTION, "255");
			}
			else {
				session->mysql_variables->client_set_value(SQL_COLLATION_CONNECTION, session->mysql_variables->server_get_value(SQL_CHARACTER_SET));
			}
			return ret;
		}

		/* The command 'set names' was called implicetely from connect_start or handshake_response.
		 * Only server variables are updated, because client does not know about this behavior of proxysql.
		 * Next time, when client will execute 'set names' we will have server variables initialized
		 */
		if (ret && !strcmp(session->client_myds->myconn->variables[SQL_CHARACTER_ACTION].value, "3")) {
			session->mysql_variables->server_set_value(SQL_CHARACTER_SET_RESULTS, session->mysql_variables->server_get_value(SQL_CHARACTER_SET));
			session->mysql_variables->server_set_value(SQL_CHARACTER_SET_CLIENT, session->mysql_variables->server_get_value(SQL_CHARACTER_SET));
			session->mysql_variables->server_set_value(SQL_CHARACTER_SET_CONNECTION, session->mysql_variables->server_get_value(SQL_CHARACTER_SET));
			session->mysql_variables->server_set_value(SQL_COLLATION_CONNECTION, session->mysql_variables->server_get_value(SQL_CHARACTER_SET));
			return ret;
		}

		ret = session->mysql_variables->verify_generic_variable(
				&session->mybe->server_myds->myconn->variables[SQL_CHARACTER_ACTION].hash,
				&session->mybe->server_myds->myconn->variables[SQL_CHARACTER_ACTION].value,
				mysql_thread___default_variables[SQL_CHARACTER_ACTION],
				&session->client_myds->myconn->variables[SQL_CHARACTER_ACTION].hash,
				session->client_myds->myconn->variables[SQL_CHARACTER_ACTION].value,
				mysql_tracked_variables[idx].status
				);
		return ret;
	}
	return false;
}

bool Names_Updater::update_server_variable(MySQL_Session* session, int idx, int &_rc) {
	auto ret = session->handler_again___status_CHANGING_CHARSET(&_rc);
	return ret;
}

bool Charset_Updater::verify_variables(MySQL_Session* session, int idx) {
	/* This verification is related to 'set charset' command only */
	if (strcmp(session->client_myds->myconn->variables[SQL_CHARACTER_ACTION].value, "2"))
		return false;

	auto ret = session->mysql_variables->verify_generic_variable(
		&session->mybe->server_myds->myconn->variables[SQL_CHARACTER_SET].hash,
		&session->mybe->server_myds->myconn->variables[SQL_CHARACTER_SET].value,
		mysql_thread___default_variables[SQL_CHARACTER_SET],
		&session->client_myds->myconn->variables[SQL_CHARACTER_SET].hash,
		session->client_myds->myconn->variables[SQL_CHARACTER_SET].value,
		mysql_tracked_variables[idx].status
	);

	/* if client and server values are different then we are going to execute 'set character set' on backed
	 * The 'set character set' was executed by client, so we are updating client variables
	 */
	if (ret) {
		session->mysql_variables->client_set_value(SQL_CHARACTER_SET_RESULTS, session->mysql_variables->server_get_value(SQL_CHARACTER_SET));
		session->mysql_variables->client_set_value(SQL_CHARACTER_SET_CLIENT, session->mysql_variables->server_get_value(SQL_CHARACTER_SET));
		return ret;
	}

	ret = session->mysql_variables->verify_generic_variable(
		&session->mybe->server_myds->myconn->variables[SQL_CHARACTER_ACTION].hash,
		&session->mybe->server_myds->myconn->variables[SQL_CHARACTER_ACTION].value,
		mysql_thread___default_variables[SQL_CHARACTER_ACTION],
		&session->client_myds->myconn->variables[SQL_CHARACTER_ACTION].hash,
		session->client_myds->myconn->variables[SQL_CHARACTER_ACTION].value,
		mysql_tracked_variables[idx].status
	);
	return ret;
}

bool Charset_Updater::update_server_variable(MySQL_Session* session, int idx, int &_rc) {
	bool no_quote = true;
	if (mysql_tracked_variables[idx].quote) no_quote = false;
	bool st = mysql_tracked_variables[idx].set_transaction;
	const char * set_var_name = mysql_tracked_variables[idx].set_variable_name;

	/* we have to convert collation id which is stored in variables to character set name which is used in
	 * the command 'set character set' which is executed on backend.
	 */
	const MARIADB_CHARSET_INFO *ci = NULL;
	ci = proxysql_find_charset_nr(atoi(session->mysql_variables->client_get_value(SQL_CHARACTER_SET)));

	auto ret = session->handler_again___status_SETTING_GENERIC_VARIABLE(&_rc, set_var_name, ci->csname, no_quote, st);
	return ret;
}
