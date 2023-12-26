use redis_module::{logging::RedisLogLevel, DetachedFromClient, ThreadSafeContext};

fn log(thread_ctx: &ThreadSafeContext<DetachedFromClient>, level: RedisLogLevel, message: &str) {
    let ctx = thread_ctx.lock();
    ctx.log(level, message);
    drop(ctx);
}

#[allow(dead_code)]
pub(crate) fn log_debug(thread_ctx: &ThreadSafeContext<DetachedFromClient>, message: &str) {
    log(thread_ctx, RedisLogLevel::Debug, message)
}

#[allow(dead_code)]
pub(crate) fn log_notice(thread_ctx: &ThreadSafeContext<DetachedFromClient>, message: &str) {
    log(thread_ctx, RedisLogLevel::Notice, message)
}

#[allow(dead_code)]
pub(crate) fn log_verbose(thread_ctx: &ThreadSafeContext<DetachedFromClient>, message: &str) {
    log(thread_ctx, RedisLogLevel::Verbose, message)
}

#[allow(dead_code)]
pub(crate) fn log_warning(thread_ctx: &ThreadSafeContext<DetachedFromClient>, message: &str) {
    log(thread_ctx, RedisLogLevel::Warning, message)
}
