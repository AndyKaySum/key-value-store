use crate::ceil_div;

///Binary search: from https://en.wikipedia.org/wiki/Binary_search_algorithm#Alternative_procedure
// pub fn binary_search<T: std::cmp::PartialOrd>(array: &[T], value: T) -> Option<usize> {
//     let (mut left, mut right) = (0, array.len() - 1);

//     while left != right {
//         let middle = ceil_div!((left + right), 2);
//         if array[middle] > value {
//             right = middle - 1;
//         } else {
//             left = middle;
//         }
//     }
//     if array[left] == value {
//         return Some(left);
//     }
//     None
// }

///Binary search: from https://en.wikipedia.org/wiki/Binary_search_algorithm#Alternative_procedure
pub fn binary_search_entries<K: std::cmp::PartialOrd, V: Clone>(
    array: &[(K, V)],
    key: K,
) -> Option<V> {
    let (mut left, mut right) = (0, array.len() - 1);

    while left != right {
        let middle = ceil_div!((left + right), 2);
        if array[middle].0 > key {
            right = middle - 1;
        } else {
            left = middle;
        }
    }
    if array[left].0 == key {
        return Some(array[left].1.clone());
    }
    None
}

///Gets rank of value (number of elements less than value in the array), from https://en.wikipedia.org/wiki/Binary_search_algorithm#Procedure_for_finding_the_leftmost_element
pub fn binary_search_leftmost<T: std::cmp::PartialOrd>(array: &[T], value: T) -> usize {
    let (mut left, mut right) = (0, array.len());

    while left < right {
        let middle = (left + right) / 2;
        if array[middle] < value {
            left = middle + 1;
        } else {
            right = middle;
        }
    }
    left
}

///From: https://en.wikipedia.org/wiki/Binary_search_algorithm#Procedure_for_finding_the_rightmost_element
pub fn binary_search_rightmost<T: std::cmp::PartialOrd>(array: &[T], value: T) -> i64 {
    let (mut left, mut right) = (0, array.len() as i64);

    while left < right {
        let middle = (left + right) / 2;
        if array[middle as usize] > value {
            right = middle;
        } else {
            left = middle + 1;
        }
    }
    right - 1
}

#[test]
fn test_leftmost_search() {
    let array = [1, 3, 5, 7, 9];
    assert_eq!(binary_search_leftmost(&array, 5), 2);
    assert_eq!(binary_search_leftmost(&array, 4), 2);
    assert_eq!(binary_search_leftmost(&array, 6), 3);

    assert_eq!(binary_search_leftmost(&array, 0), 0);
    assert_eq!(binary_search_leftmost(&array, 10), array.len());
}

#[test]
fn test_rightmost_search() {
    let array = [1, 3, 5, 7, 9];
    assert_eq!(binary_search_rightmost(&array, 5), 2);
    assert_eq!(binary_search_rightmost(&array, 4), 1);
    assert_eq!(binary_search_rightmost(&array, 6), 2);

    assert_eq!(binary_search_rightmost(&array, 0), -1);
    assert_eq!(binary_search_rightmost(&array, -20), -1);

    assert_eq!(binary_search_rightmost(&array, 10), array.len() as i64 - 1);
    assert_eq!(binary_search_rightmost(&array, 20), array.len() as i64 - 1);
}
