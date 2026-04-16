package ru.yandex.practicum.controller;

import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.dto.ProductDto;
import ru.yandex.practicum.enums.ProductCategory;
import ru.yandex.practicum.enums.ProductAvailability;
import ru.yandex.practicum.service.ProductService;

import java.util.List;

@RestController
@RequestMapping("/shopping-store")
public class ProductController {

    private final ProductService productService;

    public ProductController(ProductService productService) {
        this.productService = productService;
    }

    @GetMapping("/{id}")
    public ProductDto getProduct(@PathVariable Long id) {
        return productService.getProduct(id);
    }

    @GetMapping
    public List<ProductDto> getProducts(@RequestParam(required = false) ProductCategory category) {
        return productService.getProducts(category);
    }

    @PutMapping
    public ProductDto addOrUpdateProduct(@RequestBody ProductDto product) {
        if (product.getId() == null) {
            return productService.addProduct(product);
        } else {
            return productService.updateProduct(product);
        }
    }

    @PostMapping
    public ProductDto addProductPost(@RequestBody ProductDto product) {
        return productService.addProduct(product);
    }

    @PostMapping("/removeProductFromStore")
    public void removeProductFromStore(@RequestBody(required = false) ProductDto product,
                                       @RequestParam(required = false) Long productId) {
        Long id = productId != null ? productId : product.getId();
        productService.deactivateProduct(id);
    }

    @PostMapping("/quantityState")
    public void setQuantityState(@RequestParam(required = false) Long productId,
                                 @RequestParam(required = false) ProductAvailability quantityState,
                                 @RequestBody(required = false) ProductDto product) {
        Long id = productId != null ? productId : product.getId();
        ProductAvailability state = quantityState != null ? quantityState : product.getAvailability();

        ProductDto dto = productService.getProduct(id);
        dto.setAvailability(state);
        productService.updateProduct(dto);
    }
}