#include <ruby.h>
#include <ruby/thread.h>
#include <tiledb/tiledb.h>

// Global ctx
static tiledb_ctx_t* DEFAULT_CTX;

typedef struct rTileDB_ctx {
  tiledb_ctx_t* ctx;
} rTileDB_ctx;

typedef struct rTileDB_domain {
  tiledb_domain_t* domain;
} rTileDB_domain;

typedef struct rTileDB_dim {
  tiledb_dimension_t* dim;
} rTileDB_dim;

typedef struct rTileDB_array_schema {
  tiledb_array_schema_t* array_schema;
} rTileDB_array_schema;

typedef struct rTileDB_attr {
  tiledb_attribute_t* attr;
} rTileDB_attr;

typedef struct rTileDB_array {
  tiledb_array_t* array;
} rTileDB_array;

#define SIZE_FUNC(type) static size_t type##_size(const void* d) { return sizeof(type); }
#define ALLOC_FUNC(typename) static VALUE typename##_alloc(VALUE klass) { \
  typename* p = NULL; \
  return TypedData_Make_Struct(klass, typename, &typename##_type, p); \
}

#define RB_DATA_TYPE(typename) \
static const rb_data_type_t typename##_type = { \
  "typename", \
  {NULL, typename##_free, typename##_size, NULL}, \
  NULL, NULL, RUBY_TYPED_FREE_IMMEDIATELY, \
}

#define DEF_DATA_OBJ(typename) \
  SIZE_FUNC(typename); \
  static void typename##_free(void*p); \
  RB_DATA_TYPE(typename); \
  ALLOC_FUNC(typename)

DEF_DATA_OBJ(rTileDB_ctx);

DEF_DATA_OBJ(rTileDB_domain);
static VALUE rTileDB_domain_init(VALUE self, VALUE dims, VALUE ctx);

DEF_DATA_OBJ(rTileDB_dim);
static VALUE rTileDB_dim_init(VALUE self, VALUE name, VALUE domain, VALUE tile, VALUE rdtype, VALUE rctx);
static VALUE rTileDB_dim_name(VALUE self);

DEF_DATA_OBJ(rTileDB_array_schema);
static VALUE rTileDB_array_schema_init(VALUE self, VALUE domain, VALUE attrs, VALUE cell_order,
    VALUE tile_order, VALUE capacity, VALUE coords_filter, VALUE offsets_filter, VALUE allow_duplicates,
    VALUE sparse, VALUE rctx);

DEF_DATA_OBJ(rTileDB_attr);
static VALUE rTileDB_attr_init(VALUE self, VALUE name, VALUE rdtype, VALUE var, VALUE filters, VALUE rctx);

DEF_DATA_OBJ(rTileDB_array);
static VALUE rTileDB_array_create(VALUE self, VALUE uri, VALUE rchema, VALUE key, VALUE rctx);

// error
static VALUE raise_tiledb_error(tiledb_error_t* err)
{
  const char* msg = NULL;
  int rc = tiledb_error_message(err, &msg);
  if (rc != TILEDB_OK) {
    tiledb_error_free(&err);
    if (rc == TILEDB_OOM)
      rb_raise(rb_eNoMemError, "OOM error");
    rb_raise(rb_eRuntimeError, "error retrieving error message");
  }
  rb_raise(rb_eRuntimeError, "%s", msg);
  return Qnil;
}

static VALUE free_tiledb_error(tiledb_error_t* err)
{
  tiledb_error_free(&err);
  return Qnil;
}

inline
static void check_error(tiledb_ctx_t* ctx, int rc)
{
  if (rc == TILEDB_OK) return;
  if (rc == TILEDB_OOM) {
    rb_raise(rb_eNoMemError, "OOM error");
  }
  tiledb_error_t* err = NULL;
  int rc_last = tiledb_ctx_get_last_error(ctx, &err);
  if (rc_last != TILEDB_OK) {
    tiledb_error_free(&err);
    if (rc_last == TILEDB_OOM)
      rb_raise(rb_eNoMemError, "OOM error");
    rb_raise(rb_eRuntimeError, "error retrieving error object from ctx");
  }
  rb_ensure((VALUE(*)())raise_tiledb_error, (VALUE)err, (VALUE(*)())free_tiledb_error, (VALUE)err);
}

// Ctx
static void rTileDB_ctx_free(void* p)
{
  rTileDB_ctx* c = p;
  if (c) {
    tiledb_ctx_free(&c->ctx);
    free(c);
  }
}

static VALUE rTileDB_ctx_init(VALUE self, VALUE config)
{
  rTileDB_ctx* c = NULL;
  TypedData_Get_Struct(self, rTileDB_ctx, &rTileDB_ctx_type, c);
  //TODO: get config
  int rc = tiledb_ctx_alloc(NULL, &c->ctx);
  if (rc == TILEDB_OK)
    return self;
  else if (rc == TILEDB_OOM)
    rb_raise(rb_eNoMemError, "OOM error");
  else
    rb_raise(rb_eRuntimeError, "error initialize ctx");
}

static VALUE rTileDB_ctx_last_error(VALUE self)
{
  rTileDB_ctx* c = NULL;
  TypedData_Get_Struct(self, rTileDB_ctx, &rTileDB_ctx_type, c);

  tiledb_error_t* err = NULL;
  int rc_last = tiledb_ctx_get_last_error(c->ctx, &err);
  if (rc_last != TILEDB_OK) {
    tiledb_error_free(&err);
    if (rc_last == TILEDB_OOM)
      rb_raise(rb_eNoMemError, "[TileDB::Error] OOM error");
    rb_raise(rb_eRuntimeError, "[TileDB::Error] error retrieving error object from ctx");
  }

  const char* msg = NULL;
  int rc = tiledb_error_message(err, &msg);
  if (rc != TILEDB_OK) {
    tiledb_error_free(&err);
    if (rc == TILEDB_OOM)
      rb_raise(rb_eNoMemError, "[TileDB::Error] OOM error");
    rb_raise(rb_eRuntimeError, "[TileDB::Error] error retrieving error message");
  }
  if (!msg)
    return Qnil;
  VALUE e = rb_utf8_str_new_cstr(msg);
  return e;
}

static tiledb_ctx_t* get_ctx_default(VALUE v)
{
  tiledb_ctx_t* ctx = NULL;
  if (NIL_P(v)) {
      ctx = DEFAULT_CTX;
  } else {
    rTileDB_ctx* c = NULL;
    TypedData_Get_Struct(v, rTileDB_ctx, &rTileDB_ctx_type, c);
    ctx = c->ctx;
  }
  return ctx;
}

// Domain
static void rTileDB_domain_free(void* p)
{
  rTileDB_domain* d = p;
  if (d) {
    tiledb_domain_free(&d->domain);
    free(d);
  }
}

static VALUE rTileDB_domain_init(VALUE self, VALUE dims, VALUE rctx)
{
  Check_Type(dims, T_ARRAY);

  rTileDB_domain* dom;
  TypedData_Get_Struct(self, rTileDB_domain, &rTileDB_domain_type, dom);
  tiledb_ctx_t* ctx = get_ctx_default(rctx);
  tiledb_domain_alloc(ctx, &dom->domain);

  long i, len = RARRAY_LEN(dims);
  for (i = 0; i < len; ++i) {
    VALUE dim = RARRAY_AREF(dims, i);
    rTileDB_dim* d;
    TypedData_Get_Struct(dim, rTileDB_dim, &rTileDB_dim_type, d);
    tiledb_domain_add_dimension(ctx, dom->domain, d->dim);
  }

  return self;
}

// Dim
static void rTileDB_dim_free(void* p)
{
  rTileDB_dim* d = p;
  if (d) {
    tiledb_dimension_free(&d->dim);
    free(d);
  }
}

static VALUE rTileDB_dim_init(VALUE self, VALUE name, VALUE domain, VALUE tile, VALUE rdtype, VALUE rctx)
{
  Check_Type(domain, T_ARRAY);
  if (RARRAY_LEN(domain) != 2) {
    rb_raise(rb_eRuntimeError, "invalid domain extent, must be a pair");
  }

  if (!NIL_P(rdtype) && SYM2ID(rdtype) != rb_intern("int32")) {
    rb_notimplement();
  }

  char* nameptr = StringValueCStr(name);

  int dim_dom[2] = {NUM2INT(RARRAY_AREF(domain, 0)), NUM2INT(RARRAY_AREF(domain, 1))};
  int tiledb_dtype = TILEDB_INT32;

  int n = NUM2INT(tile);
  //TODO: mange tile type
  int* tile_extent = NULL;
  tile_extent = alloca(sizeof(int));
  *tile_extent = n;

  tiledb_ctx_t* ctx = get_ctx_default(rctx);

  rTileDB_dim* d;
  TypedData_Get_Struct(self, rTileDB_dim, &rTileDB_dim_type, d);
  int rc = tiledb_dimension_alloc(ctx, nameptr, tiledb_dtype, dim_dom, tile_extent, &d->dim);
  if (rc != TILEDB_OK) {
    d->dim = NULL;
    check_error(ctx, rc);
  }

  return self;
}

static VALUE rTileDB_dim_name(VALUE self)
{
  rTileDB_dim* d = NULL;
  const char* s = NULL;

  TypedData_Get_Struct(self, rTileDB_dim, &rTileDB_dim_type, d);
  tiledb_ctx_t* ctx = DEFAULT_CTX;
  check_error(ctx, tiledb_dimension_get_name(ctx, d->dim, &s));
  return rb_utf8_str_new_cstr(s);
}


// ArraySchema
static void rTileDB_array_schema_free(void* p)
{
  rTileDB_array_schema* a = p;
  if (a) {
    tiledb_array_schema_free(&a->array_schema);
    free(a);
  }
}

static VALUE rTileDB_array_schema_init(VALUE self, VALUE domain, VALUE attrs, VALUE cell_order,
    VALUE tile_order, VALUE capacity, VALUE coords_filter, VALUE offsets_filter, VALUE allow_duplicates,
    VALUE sparse, VALUE rctx)
{
  tiledb_ctx_t* ctx = get_ctx_default(rctx);
  tiledb_array_type_t array_type = RTEST(sparse) ? TILEDB_SPARSE : TILEDB_DENSE;

  Check_Type(attrs, T_ARRAY);

  tiledb_array_schema_t* schema = NULL;
  check_error(ctx, tiledb_array_schema_alloc(ctx, array_type, &schema));

  //TODO: set layouts from argments
  tiledb_layout_t cell_layout = TILEDB_ROW_MAJOR;
  tiledb_layout_t tile_layout = TILEDB_ROW_MAJOR;

  check_error(ctx, tiledb_array_schema_set_cell_order(ctx, schema, cell_layout));
  check_error(ctx, tiledb_array_schema_set_tile_order(ctx, schema, tile_layout));

  rTileDB_domain* d = NULL;
  TypedData_Get_Struct(domain, rTileDB_domain, &rTileDB_domain_type, d);
  //TODO: free array_schema on error
  check_error(ctx, tiledb_array_schema_set_domain(ctx, schema, d->domain));
  long i, len = RARRAY_LEN(attrs);
  for (i = 0; i < len; ++i) {
    VALUE j = RARRAY_AREF(attrs, i);
    rTileDB_attr* attr = NULL;
    TypedData_Get_Struct(j, rTileDB_attr, &rTileDB_attr_type, attr);
    tiledb_array_schema_add_attribute(ctx, schema, attr->attr);
  }

  rTileDB_array_schema* as = NULL;
  TypedData_Get_Struct(self, rTileDB_array_schema, &rTileDB_array_schema_type, as);
  as->array_schema = schema;
  return self;
}

// Attr
static void rTileDB_attr_free(void* p)
{
  rTileDB_attr* a = p;
  if (a) {
    tiledb_attribute_free(&a->attr);
    free(a);
  }
}

static VALUE rTileDB_attr_init(VALUE self, VALUE name, VALUE rdtype, VALUE var, VALUE filters, VALUE rctx)
{
  tiledb_ctx_t* ctx = get_ctx_default(rctx);
  char* nameptr = StringValueCStr(name);
  if (SYM2ID(rdtype) != rb_intern("int32")) {
    rb_notimplement();
  }

  tiledb_datatype_t dtype = TILEDB_INT32;
  int ncells = 1;

  tiledb_attribute_t* attr = NULL;
  check_error(ctx, tiledb_attribute_alloc(ctx, nameptr, dtype, &attr));
  int rc = tiledb_attribute_set_cell_val_num(ctx, attr, ncells);
  if (rc != TILEDB_OK) {
    tiledb_attribute_free(&attr);
    check_error(ctx, rc);
  }

  rTileDB_attr* a = NULL;
  TypedData_Get_Struct(self, rTileDB_attr, &rTileDB_attr_type, a);
  a->attr = attr;
  return self;
}

// Array
struct create_array_arg {
  tiledb_ctx_t* ctx;
  const char* uri;
  tiledb_array_schema_t* schema;
  tiledb_encryption_type_t key_type;
  const void* key;
  int key_length;
};

struct open_array_arg {
  tiledb_ctx_t* ctx;
  tiledb_array_t* array;
  tiledb_query_type_t query_type;
  tiledb_encryption_type_t key_type;
  const void* key;
  int key_length;
};

static void* nogvl_create_array(void* arg)
{
  struct create_array_arg* p = (struct create_array_arg*)arg;
  int rc = tiledb_array_create_with_key(p->ctx, p->uri, p->schema, p->key_type, p->key, p->key_length);
  return (void*)(VALUE)rc;
}

static void* nogvl_open_array(void* arg)
{
  struct open_array_arg* p = (struct open_array_arg*)arg;
  int rc = tiledb_array_open_with_key(p->ctx, p->array, p->query_type, p->key_type, p->key, p->key_length);
  return (void*)(VALUE)rc;
}

static VALUE rTileDB_array_create(VALUE klass, VALUE uri, VALUE rschema, VALUE key, VALUE rctx)
{
  if (!NIL_P(key)) {
    rb_notimplement();
  }
  char* uriptr = StringValueCStr(uri);
  rTileDB_ctx* ctx = NULL;
  TypedData_Get_Struct(rctx, rTileDB_ctx, &rTileDB_ctx_type, ctx);

  rTileDB_array_schema* schema = NULL;
  TypedData_Get_Struct(rschema, rTileDB_array_schema, &rTileDB_array_schema_type, schema);

  struct create_array_arg arg = {
    ctx->ctx,
    uriptr,
    schema->array_schema,
    TILEDB_NO_ENCRYPTION,
    NULL,
    0
  };
  int rc = (int)(VALUE)rb_thread_call_without_gvl(nogvl_create_array, &arg, NULL, NULL);
  if (rc != TILEDB_OK) {
    check_error(ctx->ctx, rc);
  }
  return klass;
}

static void rTileDB_array_free(void* p)
{
  rTileDB_array* a = p;
  if (a) {
    tiledb_array_free(&a->array);
    free(a);
  }
}

static int rTileDB_array_is_open(tiledb_ctx_t* ctx, tiledb_array_t* array)
{
  int open = 0;
  check_error(ctx, tiledb_array_is_open(ctx, array, &open));
  return open == 1;

}

static VALUE rTileDB_array_init(VALUE self, VALUE uri, VALUE mode, VALUE key, VALUE timestamp, VALUE attr, VALUE rctx)
{
  if (!NIL_P(key)) {
    rb_notimplement();
  }
  if (!NIL_P(attr)) {
    rb_notimplement();
  }
  const char* uriptr = StringValueCStr(uri);
  const char* modeptr = StringValuePtr(mode);
  tiledb_query_type_t query_type = TILEDB_WRITE;
  if (RSTRING_LEN(mode) == 0 || modeptr[0] != 'w')
    query_type = TILEDB_READ;
  tiledb_encryption_type_t key_type = TILEDB_NO_ENCRYPTION;
  //TODO: use timestamp
  // uint64_t ts = 0;

  rTileDB_ctx* ctx = NULL;
  TypedData_Get_Struct(rctx, rTileDB_ctx, &rTileDB_ctx_type, ctx);

  tiledb_array_t* tarray = NULL;
  check_error(ctx->ctx, tiledb_array_alloc(ctx->ctx, uriptr, &tarray));

  struct open_array_arg arg = { ctx->ctx, tarray, query_type, TILEDB_NO_ENCRYPTION, NULL, 0 };
  int rc = (int)(VALUE)rb_thread_call_without_gvl(nogvl_open_array, &arg, NULL, NULL);
  if (rc != TILEDB_OK) {
    tiledb_array_free(&tarray);
    check_error(ctx->ctx, rc);
  }

  tiledb_array_schema_t* schema = NULL;
  //TODO: nogvl
  rc = tiledb_array_get_schema(ctx->ctx, tarray, &schema);
  if (rc != TILEDB_OK) {
    tiledb_ctx_t* new_ctx = NULL;
    tiledb_ctx_alloc(NULL, &new_ctx);
    tiledb_array_close(new_ctx, tarray); // ignore error
    tiledb_ctx_free(&new_ctx);

    tiledb_array_free(&tarray);
    check_error(ctx->ctx, rc);
  }

  rTileDB_array* array = NULL;
  TypedData_Get_Struct(self, rTileDB_array, &rTileDB_array_type, array);
  array->array = tarray;
  return self;
}

static VALUE rTileDB_array_copen(VALUE self)
{
  rTileDB_array* array = NULL;
  TypedData_Get_Struct(self, rTileDB_array, &rTileDB_array_type, array);
  VALUE rctx = rb_ivar_get(self, rb_intern("@ctx"));
  rTileDB_ctx* ctx = NULL;
  TypedData_Get_Struct(rctx, rTileDB_ctx, &rTileDB_ctx_type, ctx);

  if (rTileDB_array_is_open(ctx->ctx, array->array))
    return Qtrue;
  return Qfalse;
}

static VALUE rTileDB_array_close(VALUE self)
{
  rTileDB_array* array = NULL;
  TypedData_Get_Struct(self, rTileDB_array, &rTileDB_array_type, array);

  VALUE rctx = rb_ivar_get(self, rb_intern("@ctx"));
  rTileDB_ctx* ctx = NULL;
  TypedData_Get_Struct(rctx, rTileDB_ctx, &rTileDB_ctx_type, ctx);
  // //TODO: nogvl
  check_error(ctx->ctx, tiledb_array_close(ctx->ctx, array->array));
  rb_iv_set(self, "@schema", Qnil);
  return self;
}

// build classes
static void define_ctx(VALUE module)
{
  VALUE cCtx = rb_define_class_under(module, "Ctx", rb_cData);
  rb_define_alloc_func(cCtx, rTileDB_ctx_alloc);
  rb_define_method(cCtx, "cinit", rTileDB_ctx_init, 1);
  rb_define_method(cCtx, "last_error", rTileDB_ctx_last_error, 0);
}

static void define_domain(VALUE module)
{
  VALUE domain = rb_define_class_under(module, "Domain", rb_cData);
  rb_define_alloc_func(domain, rTileDB_domain_alloc);
  rb_define_private_method(domain, "cinit", rTileDB_domain_init, 2);
}

static void define_dim(VALUE module)
{
  VALUE dim = rb_define_class_under(module, "Dim", rb_cData);
  rb_define_alloc_func(dim, rTileDB_dim_alloc);
  rb_define_private_method(dim, "cinit", rTileDB_dim_init, 5);
  rb_define_method(dim, "name", rTileDB_dim_name, 0);
}

static void define_array_schema(VALUE module)
{
  VALUE as = rb_define_class_under(module, "ArraySchema", rb_cData);
  rb_define_alloc_func(as, rTileDB_array_schema_alloc);
  rb_define_private_method(as, "cinit", rTileDB_array_schema_init, 10);
}

static void define_attr(VALUE module)
{
  VALUE a = rb_define_class_under(module, "Attr", rb_cData);
  rb_define_alloc_func(a, rTileDB_attr_alloc);
  rb_define_private_method(a, "cinit", rTileDB_attr_init, 5);
}

static void define_array(VALUE module)
{
  VALUE a = rb_define_class_under(module, "Array", rb_cData);
  rb_define_alloc_func(a, rTileDB_array_alloc);
  rb_define_singleton_method(a, "create_impl", rTileDB_array_create, 4);
  rb_define_private_method(a, "cinit", rTileDB_array_init, 6);
  rb_define_private_method(a, "open?", rTileDB_array_copen, 0);
  rb_define_method(a, "close", rTileDB_array_close, 0);
}

static VALUE rTileDB_version(VALUE module)
{
  int32_t major = 0;
  int32_t minor = 0;
  int32_t rev = 0;
  tiledb_version(&major, &minor, &rev);
  VALUE a = rb_ary_new_capa(3);
  rb_ary_store(a, 0, INT2FIX(major));
  rb_ary_store(a, 1, INT2FIX(minor));
  rb_ary_store(a, 2, INT2FIX(rev));
  return a;
}

void Init_tiledb(void)
{
  VALUE mTileDB = rb_define_module("TileDB");
  rb_define_module_function(mTileDB, "runtime_version", rTileDB_version, 0);
  rb_define_const(mTileDB, "ROW_MAJOR", TILEDB_ROW_MAJOR);
  rb_define_const(mTileDB, "COL_MAJOR", TILEDB_COL_MAJOR);
  rb_define_const(mTileDB, "GLOBAL_ORDER", TILEDB_GLOBAL_ORDER);
  rb_define_const(mTileDB, "UNORDERED", TILEDB_UNORDERED);

  tiledb_ctx_alloc(NULL, &DEFAULT_CTX);

  define_ctx(mTileDB);
  define_domain(mTileDB);
  define_dim(mTileDB);
  define_array_schema(mTileDB);
  define_attr(mTileDB);
  define_array(mTileDB);
}
