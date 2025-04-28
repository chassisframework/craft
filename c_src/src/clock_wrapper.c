#include <erl_nif.h>
#include <time.h>

static ERL_NIF_TERM get_monotonic_time(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    struct timespec ts;
    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        return enif_make_atom(env, "error");
    }

    ERL_NIF_TERM sec = enif_make_int64(env, ts.tv_sec);
    ERL_NIF_TERM nsec = enif_make_int64(env, ts.tv_nsec);

    return enif_make_tuple2(env, sec, nsec);
}

static ErlNifFunc nif_funcs[] = {
    {"get_monotonic_time", 0, get_monotonic_time}
};

ERL_NIF_INIT(Elixir.ClockBound.Native, nif_funcs, NULL, NULL, NULL, NULL)
