// Copyright 2023-2024 Michael Sawka
// MIT License (see LICENSE)

// Simple wrapper around sqlx.Tx to provide error handling and automatic commit/rollback
package txwrap

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jmoiron/sqlx"
)

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

func WithTxRtn[RT any](ctx context.Context, db *sqlx.DB, fn func(tx *TxWrap) (RT, error)) (RT, error) {
	var rtn RT
	txErr := WithTx(ctx, db, func(tx *TxWrap) error {
		temp, err := fn(tx)
		if err != nil {
			return err
		}
		rtn = temp
		return nil
	})
	return rtn, txErr
}

// Main transaction wrapper. If any database call fails, or an error is returned from
// 'fn' then the transation will be rolled back and the first error will be returned.
// Otherwise the transaction will be committed and WithTx will return nil.
//
// Note that WithTx *can* be nested.  If there is already an error WithTx will immediately
// return that error.  Otherwise it will use the existing outer TxWrap object.  Note that
// this will *not* run a nested DB transation.  Begin and Commit/Rollback will only
// be called once for the *outer* transaction.
func WithTx(ctx context.Context, db *sqlx.DB, fn func(tx *TxWrap) error) (rtnErr error) {
	var txWrap *TxWrap
	ctxVal := ctx.Value(txWrapKey{})
	if ctxVal != nil {
		txWrap = ctxVal.(*TxWrap)
		if txWrap.Err != nil {
			return txWrap.Err
		}
	}
	if txWrap == nil {
		if db == nil {
			return fmt.Errorf("invalid nil DB passed to WithTxDB")
		}
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
	var rtnStr *string
	tx.Get(&rtnStr, query, args...)
	if rtnStr == nil {
		return ""
	}
	return *rtnStr
}

func (tx *TxWrap) GetFloat64(query string, args ...interface{}) float64 {
	var rtnFloat *float64
	tx.Get(&rtnFloat, query, args...)
	if rtnFloat == nil {
		return 0
	}
	return *rtnFloat
}

func (tx *TxWrap) GetByteArr(query string, args ...interface{}) []byte {
	var rtnByteArr *[]byte
	tx.Get(&rtnByteArr, query, args...)
	if rtnByteArr == nil {
		return nil
	}
	return *rtnByteArr
}

func (tx *TxWrap) GetBool(query string, args ...interface{}) bool {
	var rtnBool bool
	tx.Get(&rtnBool, query, args...)
	return rtnBool
}

func GetGeneric[RT any](tx TxWrap, query string, args ...interface{}) RT {
	var rtn RT
	tx.Get(&rtn, query, args...)
	return rtn
}

func (tx *TxWrap) SelectStrings(query string, args ...interface{}) []string {
	var rtnArr []string
	tx.Select(&rtnArr, query, args...)
	return rtnArr
}

func (tx *TxWrap) GetInt(query string, args ...interface{}) int {
	var rtnInt *int
	tx.Get(&rtnInt, query, args...)
	if rtnInt == nil {
		return 0
	}
	return *rtnInt
}

func (tx *TxWrap) GetInt64(query string, args ...interface{}) int64 {
	var rtnInt *int64
	tx.Get(&rtnInt, query, args...)
	if rtnInt == nil {
		return 0
	}
	return *rtnInt
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
}

func (tx *TxWrap) SetErr(err error) {
	if tx.Err == nil {
		tx.Err = err
	}
}
