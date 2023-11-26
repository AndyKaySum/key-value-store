#[macro_export]
///Cheap integer ceil division: ceil(a/b)
macro_rules! ceil_div {
    ($numerator:expr, $denominator:expr) => {
        ($numerator + $denominator - 1) / $denominator
    };
}
