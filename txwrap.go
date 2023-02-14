// Copyright 2023 Mike Sawka
// MIT License (see LICENSE)

// Simple wrapper around sqlx.Tx to provide error handling and automatic commit/rollback
package txwrap

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jmoiron/sqlx"
)

// Allows for custom database get/release logic
// If GetDB returns a DB, ReleaseDB is guaranteed to be called once the
// transaction has been committed or rolledback.
type DBGetter interface {
	GetDB(ctx context.Context) (*sqlx.DB, error)
	ReleaseDB(*sqlx.DB)
}

// Main TxWrap data-structure.  Wraps the sqlx.Tx interface.  Once an error
// is returned from one of the DB calls, no future calls will run and the transaction
// will be rolled back.
//
// Notes:
// * Can get the raw sqlx.Tx or Err directly from the struct
// * TxWrap is not thread-safe, must be synchronized externally to be used by multiple go-routines
// * If you use sqlx.Rows, sqlx.Row, or sqlx.Stmt directly you'll have to implement and return your errors manually.
type TxWrap struct {
	Txx *sqlx.Tx
	Err error

	ctx context.Context
}

// context-key
type txWrapKey struct{}

// Checks to see if the given Context is running a TxWrap transaction
func IsTxWrapContext(ctx context.Context) bool {
	ctxVal := ctx.Value(txWrapKey{})
	return ctxVal != nil
}

// Simple implementation of DBGetter for a sqlx.DB
type SimpleDBGetter sqlx.DB

func (db *SimpleDBGetter) GetDB(ctx context.Context) (*sqlx.DB, error) {
	return (*sqlx.DB)(db), nil
}

func (db *SimpleDBGetter) ReleaseDB(instance *sqlx.DB) {
}

// Simple transaction wrapper.  If any database call fails, or an error is returned from
// 'fn' then the transation will be rolled back and the first error will be returned.
// Otherwise the transaction will be committed and WithTx will return nil.
//
// Calls DBGWithTx with 'db' wrapped in SimpleDBGetter
func WithTx(ctx context.Context, db *sqlx.DB, fn func(tx *TxWrap) error) error {
	if db == nil {
		return fmt.Errorf("invalid nil DB passed to WithTx")
	}
	dbg := (*SimpleDBGetter)(db)
	return DBGWithTx(ctx, dbg, fn)
}

// Main transaction wrapper. If any database call fails, or an error is returned from
// 'fn' then the transation will be rolled back and the first error will be returned.
// Otherwise the transaction will be committed and WithTx will return nil.
//
// Note that WithTx *can* be nested.  If there is already an error WithTx will immediately
// return that error.  Otherwise it will use the existing outer TxWrap object.  Note that
// this will *not* run a nested DB transation.  Begin and Commit/Rollback will only
// be called once for the *outer* transaction.
func DBGWithTx(ctx context.Context, dbWrap DBGetter, fn func(tx *TxWrap) error) (rtnErr error) {
	var txWrap *TxWrap
	ctxVal := ctx.Value(txWrapKey{})
	if ctxVal != nil {
		txWrap = ctxVal.(*TxWrap)
		if txWrap.Err != nil {
			return txWrap.Err
		}
	}
	if txWrap == nil {
		db, getDBErr := dbWrap.GetDB(ctx)
		if getDBErr != nil {
			return getDBErr
		}
		if db == nil {
			return fmt.Errorf("GetDB returned nil DB")
		}
		defer dbWrap.ReleaseDB(db)
		tx, beginErr := db.BeginTxx(ctx, nil)
		if beginErr != nil {
			return beginErr
		}
		txWrap = &TxWrap{Txx: tx, ctx: ctx}
		defer func() {
			if p := recover(); p != nil {
				txWrap.Txx.Rollback()
				panic(p)
			}
			if rtnErr != nil {
				txWrap.Txx.Rollback()
			} else {
				rtnErr = txWrap.Txx.Commit()
			}
		}()
	}
	fnErr := fn(txWrap)
	if txWrap.Err == nil && fnErr != nil {
		txWrap.Err = fnErr
	}
	if txWrap.Err != nil {
		return txWrap.Err
	}
	return nil
}

// Returns the TxWrap Context (with the txWrapKey).
// Must use this Context for nested calls to TxWrap
func (tx *TxWrap) Context() context.Context {
	return context.WithValue(tx.ctx, txWrapKey{}, tx)
}

func (tx *TxWrap) NamedExec(query string, arg interface{}) sql.Result {
	if tx.Err != nil {
		return nil
	}
	result, err := tx.Txx.NamedExecContext(tx.ctx, query, arg)
	if err != nil {
		tx.Err = err
	}
	return result
}

func (tx *TxWrap) Exec(query string, args ...interface{}) sql.Result {
	if tx.Err != nil {
		return nil
	}
	result, err := tx.Txx.ExecContext(tx.ctx, query, args...)
	if err != nil {
		tx.Err = err
	}
	return result
}

// Returns false if there is an error or the query returns sql.ErrNoRows.
// Otherwise if there is at least 1 matching row, returns true.
func (tx *TxWrap) Exists(query string, args ...interface{}) bool {
	var dest interface{}
	return tx.Get(&dest, query, args...)
}

func (tx *TxWrap) GetString(query string, args ...interface{}) string {
	var rtnStr string
	tx.Get(&rtnStr, query, args...)
	return rtnStr
}

func (tx *TxWrap) GetBool(query string, args ...interface{}) bool {
	var rtnBool bool
	tx.Get(&rtnBool, query, args...)
	return rtnBool
}

func (tx *TxWrap) SelectStrings(query string, args ...interface{}) []string {
	var rtnArr []string
	tx.Select(&rtnArr, query, args...)
	return rtnArr
}

func (tx *TxWrap) GetInt(query string, args ...interface{}) int {
	var rtnInt int
	tx.Get(&rtnInt, query, args...)
	return rtnInt
}

// If there is an error or sql.ErrNoRows will return false, otherwise true.
// Note that sql.ErrNoRows will *not* error out the TxWrap.
func (tx *TxWrap) Get(dest interface{}, query string, args ...interface{}) bool {
	if tx.Err != nil {
		return false
	}
	err := tx.Txx.GetContext(tx.ctx, dest, query, args...)
	if err != nil && err == sql.ErrNoRows {
		return false
	}
	if err != nil {
		tx.Err = err
		return false
	}
	return true
}

func (tx *TxWrap) Select(dest interface{}, query string, args ...interface{}) {
	if tx.Err != nil {
		return
	}
	err := tx.Txx.SelectContext(tx.ctx, dest, query, args...)
	if err != nil {
		tx.Err = err
	}
	return
}

func (tx *TxWrap) SelectMaps(query string, args ...interface{}) []map[string]interface{} {
	if tx.Err != nil {
		return nil
	}
	rows, err := tx.Txx.QueryxContext(tx.ctx, query, args...)
	if err != nil {
		tx.Err = err
		return nil
	}
	var rtn []map[string]interface{}
	for rows.Next() {
		m := make(map[string]interface{})
		err = rows.MapScan(m)
		if err != nil {
			tx.Err = err
			return nil
		}
		rtn = append(rtn, m)
	}
	return rtn
}

func (tx *TxWrap) GetMap(query string, args ...interface{}) map[string]interface{} {
	if tx.Err != nil {
		return nil
	}
	row := tx.Txx.QueryRowxContext(tx.ctx, query, args...)
	m := make(map[string]interface{})
	err := row.MapScan(m)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil
		}
		tx.Err = err
		return nil
	}
	return m
}

// Runs a function iff there has been no error
func (tx *TxWrap) Run(fn func() error) {
	if tx.Err != nil {
		return
	}
	err := fn()
	if err != nil {
		tx.Err = err
	}
	return
}
